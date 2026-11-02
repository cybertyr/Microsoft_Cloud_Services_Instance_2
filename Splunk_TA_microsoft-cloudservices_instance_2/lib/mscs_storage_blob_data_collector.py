#!/usr/bin/python
#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
import codecs
import json
import sys
import time
import gzip
import io

from azure.core.exceptions import HttpResponseError
from azure.storage.blob import BlobClient, StorageStreamDownloader, BlobProperties
from solnlib import utils

import mscs_checkpoint_util as cutil
import mscs_consts
import mscs_logger as logger
import mscs_storage_service as mss
import mscs_util
import mscs_storage_dispatcher as msd
import splunktaucclib.common.log as stulog  # pylint: disable=import-error
import splunktaucclib.data_collection.ta_data_client as dc  # pylint: disable=import-error

from solnlib.observability import StanzaObservabilityRecorder
from splunk_ta_mscs.models import ProxyConfig, AzureStorageAccountConfig
from splunksdc.utils import parseCSVLine


def running_task(
    all_conf_contents,
    meta_config,
    task_config,
    ckpt,
    last_seen_lock_id,
    canceled,
    data_writer,
    logger_prefix,
    confirm_checkpoint_lock,
    checkpointer,
    proxy_config,
    storage_account_config: AzureStorageAccountConfig,
):
    try:
        data_collector = StorageBlobDataCollector(
            all_conf_contents,
            meta_config,
            task_config,
            ckpt,
            last_seen_lock_id,
            canceled,
            data_writer,
            confirm_checkpoint_lock,
            checkpointer,
            proxy_config,
            storage_account_config,
        )

        data_collector.collect_data()
    except Exception as e:
        stulog.logger.exception(
            f'{logger_prefix}, message="Unknown Exception while running task: {e}"',
            exc_info=e,
        )


class StorageBlobDataCollector(mss.BlobStorageService):
    DEFAULT_BATCH_SIZE = 8192

    DEFAULT_DECODING = "utf-8"

    def __init__(
        self,
        all_conf_contents,
        meta_config,
        task_config,
        ckpt,
        last_seen_lock_id,
        canceled,
        data_writer,
        confirm_checkpoint_lock,
        checkpointer,
        proxy_config: ProxyConfig,
        storage_account_config: AzureStorageAccountConfig,
    ):
        super(StorageBlobDataCollector, self).__init__(
            all_conf_contents=all_conf_contents,
            meta_config=meta_config,
            task_config=task_config,
            proxy_config=proxy_config,
            storage_account_config=storage_account_config,
            logger=logger.logger_for(
                self._get_logger_prefix(
                    task_config=task_config, all_conf_contents=all_conf_contents
                )
            ),
        )

        self._ckpt = ckpt if ckpt else {}
        self._last_seen_lock_id = last_seen_lock_id
        self._canceled = canceled
        self._data_writer = data_writer
        self._container_name = None
        # assign by blob dispathcer
        self._blob_name = None
        self._snapshot = None
        self._blob_type = None
        self._last_modified = None
        self._blob_creation_time = None
        self._blob_mode = None
        self._blob_compression = None
        self._dont_reupload_blob_same_size = None
        self._blob_size = None
        self._inflate_method = None
        self._index = None
        self._sourcetype = None
        self._batch_size = None
        self._etag = None
        # init self._decoder
        self._decoder = None
        self._parse_csv_with_delimiter = None
        self._parse_csv_with_header = None

        # wrapper to confirm the lock
        self._confirm_checkpoint_lock = confirm_checkpoint_lock

        self._obs = StanzaObservabilityRecorder(
            "storage-blob", self._logger, task_config[mscs_consts.STANZA_NAME]
        )

        self._init_from_task_config()
        self._ckpt_name = cutil.get_blob_checkpoint_name(
            self._container_name, self._blob_name, self._snapshot
        )
        self._checkpointer = checkpointer

    def release_lock(self):
        """
        Releases the lock in case of exception
        """
        try:
            # this prevents data loss, but it might cause data duplication.
            # duplication can occur because inconsistent read after write can result in reading not an up-to-date value
            received_bytes_commited = self._checkpointer.get(self._ckpt_name).get(
                mscs_consts.RECEIVED_BYTES, 0
            )
            received_bytes_local = self._ckpt.get(mscs_consts.RECEIVED_BYTES, 0)

            self._ckpt["lock"] = 0
            self._ckpt[mscs_consts.RECEIVED_BYTES] = received_bytes_commited
            self._checkpointer.update(self._ckpt_name, self._ckpt)
            lock = self._ckpt["lock"]
            lock_id = self._ckpt["lock_id"]
            self._logger.warning(
                f'message="Released blob checkpoint lock on exception while processing blob." '
                f"lock={lock}, lock_id={lock_id}, "
                f"received_bytes_commited={received_bytes_commited}, received_bytes_local={received_bytes_local}"
            )
        except KeyError:
            raise msd.BlobKeyError(self._ckpt_name)

    @mscs_util.log_time_of_execution
    def collect_data(self):
        # check whether any other input locked the blob this can happen because
        # KV Store does not have "read after write" consistency but offers eventual consistency
        # lock acquired by another input then return
        if not self._confirm_ckpt_lock():
            self._logger.warning('message="Could not confirm checkpointer lock"')
            return

        try:
            self._do_collect_data()
        except mscs_util.IsCancelledException as e:
            self._logger.exception(
                'message="Collection received cancel signal. Releasing lock."',
                exc_info=e,
            )
            self.release_lock()
        except Exception as e:
            self._logger.exception(
                'message="Error occurred in collecting data"', exc_info=e
            )
            self.release_lock()

    @mscs_util.log_time_of_execution
    def _confirm_ckpt_lock(self):
        """Confirm checkpoint lock
        Double checks if the key in kv store is still locked by
        this process.

        Raises:
            msd.BlobKeyBusy: if already used by other process
        """
        try:
            return mscs_util.retry_fn(
                lambda: self._confirm_checkpoint_lock(
                    expected_ckpt=self._ckpt,
                    last_seen_lock_id=self._last_seen_lock_id,
                    storage_info=self._task_config,
                ),
                logger=self._logger,
                exceptions=(msd.BlobKeyNotFound, msd.BlobKeyNotUpdated),
                tries=7,
                delay=1,
                backoff=2.5,  # 162 seconds total
            )
        except msd.BlobKeyException:
            return False
        except Exception as e:
            self.release_lock()
            self._logger.warning(
                f'message="Failed to confirm checkpoint lock. '
                f'checkpoint={self._ckpt}" '
                f'last_seen_lock_id={self._last_seen_lock_id}"',
                exc_info=e,
            )
            return False

    def _assert_not_cancelled(self):
        if self._canceled.is_set():
            raise mscs_util.IsCancelledException("Is cancelled.")

    def _do_collect_data(self):
        self._process_ckpt()

        self._assert_not_cancelled()

        # get blob service
        blob_client = self.get_blob_client(self._blob_name)
        start_offset = self._ckpt[mscs_consts.RECEIVED_BYTES]
        while self._ckpt[mscs_consts.RECEIVED_BYTES] != self._blob_size:
            self._assert_not_cancelled()

            offset = self._ckpt[mscs_consts.RECEIVED_BYTES]
            append_mode = (
                self._blob_mode == mss.BlobModeType.APPEND
                or self._blob_type == mss.BlobType.APPEND_BLOB
            )

            try:
                if append_mode:
                    length = min(self._blob_size - offset, self._batch_size)
                    blob = self._get_blob(
                        blob_client=blob_client, offset=offset, length=length
                    )
                else:
                    blob = self._get_blob(
                        blob_client=blob_client,
                        offset=0,
                        length=self._blob_size,
                        if_match=self._etag,
                    )

                # @TODO: Duplicated logic: _process_ckpt
                blob_properties: BlobProperties = blob.properties
                self._logger.debug(
                    f'message="Blob size from properties" blob_size_bytes={blob_properties.size}'
                )
                stream_blob_created = blob_properties.creation_time.isoformat("T")
                if append_mode:
                    if self._blob_creation_time < stream_blob_created:
                        self._logger.warning(
                            f'message="Append mode|blob. Scheduled file re-uploaded during download. '
                            f'{self._blob_creation_time} < {stream_blob_created}"'
                        )

                    stream_blob_len = self._get_blob_len(blob)
                    if self._blob_size > stream_blob_len:
                        self._logger.warning(
                            f'message="Append mode|blob. Scheduled file shrunk in size. '
                            f'{self._blob_size} > {stream_blob_len}"'
                        )
                else:
                    # regular RandomBlob will fail with etag mismatch exception
                    pass

                blob_content = blob.readall()

                # get decoded blob contents
                content = self._get_blob_content(
                    first_process_blob=offset == 0, blob_content=blob_content
                )

                # if lock aquired by another input in between data processing then return
                if not self._confirm_ckpt_lock():
                    self._logger.warning(
                        'message="Lock aquired by another input in between processing."',
                    )
                    break

            except Exception as e:
                """
                Blob got deleted in the interval between blob_client.download_blob() calls, connection exceptions, etc
                """
                self._logger.warning('message="Failed to collect blob."', exc_info=e)
                self.release_lock()
                break

            offset += len(blob_content)
            is_finished = offset == self._blob_size

            self._ckpt[mscs_consts.RECEIVED_BYTES] = offset
            self._ckpt[mscs_consts.IS_COMPLETED] = 1 if is_finished else 0
            self._ckpt["lock"] = (
                0
                if is_finished
                else time.time() + mscs_consts.BLOB_SCHEDULER_BLOCK_TIME_APPEND
            )

            if content:
                self._logger.debug("Decoded blob contents")
                if self._inflate_method and self._parse_csv_with_header:
                    events, event_bytes = self._build_csv_events(
                        content=content, is_finished=is_finished
                    )
                    self._data_writer.write_events_and_ckpt(
                        events=events, key=self._ckpt_name, ckpt=self._ckpt
                    )
                    self._obs.record(len(events), event_bytes)
                else:
                    event = self._build_event(content=content, is_finished=is_finished)
                    self._data_writer.write_events_and_ckpt(
                        events=[event], key=self._ckpt_name, ckpt=self._ckpt
                    )
                    self._obs.record(1, len(content.encode()))
                start_offset = offset
            else:
                self._logger.debug("Blob contents not decoded")

                # start from the last successful decoding in case of crash
                ckpt = self._ckpt.copy()
                ckpt[mscs_consts.IS_COMPLETED] = 0
                ckpt[mscs_consts.RECEIVED_BYTES] = start_offset
                self._data_writer.write_ckpt(self._ckpt_name, self._ckpt)

                if is_finished:
                    self._logger.warning(
                        f'message="Blob contents not decoded for the last chunk of data. '
                        f'Next iteration will start from start_offset={start_offset} instead of offset={offset}"'
                    )

    @mscs_util.log_time_of_execution
    def _get_blob(
        self, blob_client: BlobClient, length: int, offset: int = 0, if_match=None
    ) -> StorageStreamDownloader:
        try:
            self._logger.debug(
                f"start_range: {offset}, end_range: {offset + length - 1}, size: {self._blob_size}"
            )
            blob_stream_downloader = blob_client.download_blob(
                snapshot=self._snapshot,
                offset=offset,
                length=length,
                if_match=if_match,
            )

            return blob_stream_downloader

        except Exception as e:
            exception_cls_name = e.__class__.__name__

            if isinstance(e, HttpResponseError):
                error_code = e.error_code if hasattr(e, "error_code") else None
                exception_cls_name = f"{exception_cls_name}.{error_code}"
                if error_code == "InvalidRange":
                    """
                    File we were downloading shrunk in size, i.e. is new.
                    Restart consumption from zero.
                    """

                    blob_properties = blob_client.get_blob_properties(
                        snapshot=self._snapshot
                    )
                    self._logger.warning(
                        f'message="{exception_cls_name}: Blob shrunk in size {blob_properties.size} <= expected length: {self._blob_size}."',
                        exc_info=e,
                    )
            raise e

    def _get_codeset(self, head_data=None):
        # checkpoint
        if self._ckpt.get(mscs_consts.CODESET):
            return self._ckpt[mscs_consts.CODESET]
        # check bom
        if head_data:
            with_bom = mscs_util.check_bom(head_data)
            if with_bom:
                return with_bom[0]
            self._logger.debug("Failed to detect the bom header.")
        # conf file
        if self._task_config.get(mscs_consts.DECODING):
            return self._task_config.get(mscs_consts.DECODING)
        # sys default
        if sys.getdefaultencoding():
            return sys.getdefaultencoding()
        return StorageBlobDataCollector.DEFAULT_DECODING

    def _set_decoder(self, first_process_blob, blob_content):
        if first_process_blob:
            codeset = self._get_codeset(blob_content)
            self._logger.debug("The codeset=%s", codeset)

            try:
                self._decoder = codecs.getincrementaldecoder(codeset)(errors="replace")
            except LookupError:
                self._logger.error(
                    f'message="charset={codeset} raise LookupError. Charset will be set to utf-8"',
                )
                codeset = StorageBlobDataCollector.DEFAULT_DECODING
                self._decoder = codecs.getincrementaldecoder(codeset)(errors="replace")
            self._ckpt[mscs_consts.CODESET] = codeset
            self._logger.debug("charset=%s", codeset)

        else:
            codeset = self._get_codeset()
            self._logger.debug("The codeset=%s", codeset)
            if not self._decoder:
                self._decoder = codecs.getincrementaldecoder(codeset)(errors="replace")

    def _inflate_method_gzip(self, content):
        compressed_size = len(content)
        self._logger.debug(
            f'message="Starting gzip decompression." '
            f"blob_name={self._blob_name} compressed_size_bytes={compressed_size}"
        )
        try:
            with io.BytesIO() as result:
                with io.BytesIO(content) as compressed:
                    with gzip.GzipFile(mode="r", fileobj=compressed) as inflated:
                        part = inflated.read()
                        while part:
                            result.write(part)
                            part = inflated.read()
                        result.flush()
                decompressed = result.getvalue()
                decompressed_size = len(decompressed)
                expansion_ratio = (
                    round(decompressed_size / compressed_size, 2)
                    if compressed_size > 0
                    else 0
                )
                self._logger.info(
                    f'message="Blob has been uncompressed with gzip" '
                    f"blob_name={self._blob_name} "
                    f"compressed_size_bytes={compressed_size} "
                    f"decompressed_size_bytes={decompressed_size} "
                    f"expansion_ratio={expansion_ratio}"
                )
                return decompressed
        except Exception as e:
            self._logger.error(
                f'message="Unexpected error while uncompressing blob."',
                exc_info=e,
            )
            return content

    def _assign_inflate_method(self):
        if self._blob_compression == mscs_consts.BLOB_NOT_COMPRESSED:
            return None

        if self._blob_compression == mscs_consts.BLOB_COMPRESSION_GZIP:
            return self._inflate_method_gzip

        if self._blob_compression == mscs_consts.BLOB_COMPRESSION_EXT:
            if self._blob_name.endswith(".gz"):
                return self._inflate_method_gzip

            self._logger.debug(
                f"Blob {self._blob_name} name extension is not a supported compression type"
            )
            return None

        self._logger.error(
            f'message="The selected blob_compression={self._blob_compression} is not supported"'
        )
        return None

    def _get_blob_content(self, first_process_blob, blob_content):
        if callable(self._inflate_method):
            self._logger.debug(
                f'message="Inflating compressed blob." '
                f"blob_name={self._blob_name} blob_size_bytes={self._blob_size} "
                f"chunk_size_bytes={len(blob_content)}"
            )
            blob_content = self._inflate_method(blob_content)
        self._set_decoder(first_process_blob, blob_content)
        decoded_content = self._decoder.decode(blob_content)

        # Parse compressed CSV files (e.g., .csv.gz, .tsv.gz)
        if self._inflate_method and self._parse_csv_with_header:
            self._logger.info(f"Processing CSV content for file: {self._blob_name}")
            return self._process_csv_content(decoded_content)
        return decoded_content

    def _process_csv_content(self, content):
        """Process CSV content by parsing lines and converting to JSON format.

        Parses CSV content line by line, using the first line as headers and
        converting subsequent data rows to JSON objects.

        Args:
            content (str): CSV content as string

        Returns:
            str: JSON events separated by newlines
        """
        if not content.strip():
            self._logger.warning("CSV content is empty after stripping")
            return ""

        self._logger.debug(f"Using CSV delimiter: `{self._parse_csv_with_delimiter}`")
        headers = None
        row_count = 0
        rows_error_count = 0

        content_io = io.StringIO(content)

        result_buffer = io.StringIO()

        for csv_line in content_io:
            # Skip empty lines
            if not csv_line.strip():
                continue

            try:
                csv_record, headers = parseCSVLine(
                    csv_line, headers, self._parse_csv_with_delimiter
                )
                if csv_record:
                    json_event = json.dumps(csv_record)
                    result_buffer.write(json_event)
                    result_buffer.write("\n")
                    row_count += 1
            except Exception as ex:
                row_count += 1
                rows_error_count += 1
                self._logger.error(
                    f"CSV parsing failed at row {row_count}. Reason: {ex}"
                )
                continue

        successful_rows = row_count - rows_error_count
        self._logger.info(
            f"Delimited File Parser: {self._blob_name} processed {successful_rows} rows successfully. "
            f"There are {rows_error_count} errors. Delimiter: '{self._parse_csv_with_delimiter}'"
        )

        return result_buffer.getvalue()

    def _init_from_task_config(self):
        self._container_name = self._task_config[mscs_consts.CONTAINER_NAME]
        self._blob_name = self._task_config[mscs_consts.BLOB_NAME]

        # @TODO: remove. Snapshot is nowhere mentioned in the docs, API. Only works on-prem through inputs.conf
        #  modification.
        self._snapshot = self._task_config.get(mscs_consts.SNAPSHOT)
        if not self._snapshot:
            self._snapshot = None

        self._blob_type = self._task_config[mscs_consts.BLOB_TYPE]
        self._last_modified = self._task_config[mscs_consts.LAST_MODIFIED]
        self._blob_creation_time = self._task_config[mscs_consts.BLOB_CREATION_TIME]
        self._blob_size = self._task_config[mscs_consts.BLOB_SIZE]
        self._etag = self._task_config[mscs_consts.ETAG]
        self._blob_mode = self._task_config.get(
            mscs_consts.BLOB_MODE, mss.BlobModeType.RANDOM
        )
        self._logger.debug("The blob_mode=%s", self._blob_mode)

        self._blob_compression = (
            self._task_config.get(mscs_consts.BLOB_COMPRESSION)
            or mscs_consts.BLOB_NOT_COMPRESSED
        )
        self._parse_csv_with_delimiter = self._task_config.get(
            mscs_consts.PARSE_CSV_WITH_DELIMITER
        )
        self._parse_csv_with_header = utils.is_true(
            self._task_config.get(mscs_consts.PARSE_CSV_WITH_HEADER)
        )
        self._inflate_method = self._assign_inflate_method()
        self._logger.debug(
            f"The blob_compression={self._blob_compression} inflate_method={self._inflate_method}"
        )
        self._dont_reupload_blob_same_size = utils.is_true(
            self._task_config.get(mscs_consts.DONT_REUPLOAD_BLOB_SAME_SIZE)
        )
        if (
            self._blob_type == mss.BlobType.APPEND_BLOB
            and self._blob_mode == mss.BlobModeType.RANDOM
        ):
            self._logger.debug(
                "Set blob_mode=random for AppendBlob doesn't have any effect, "
                "this add-on will treat AppendBlob as blob_mode=append"
            )

        self._index = self._task_config[mscs_consts.INDEX]
        self._sourcetype = self._task_config[mscs_consts.SOURCETYPE]
        self._batch_size = int(
            mscs_util.find_config_in_settings(
                mscs_consts.GET_BLOB_BATCH_SIZE,
                mscs_consts.GET_BLOB_BATCH_SIZE_DEFAULT_VALUE,
                self._task_config,
                self._all_conf_contents[mscs_consts.GLOBAL_SETTINGS][
                    mscs_consts.PERFORMANCE_TUNING_SETTINGS
                ],
            )
        )
        if self._batch_size <= 1:
            self._logger.warning(
                f'message="{mscs_consts.GET_BLOB_BATCH_SIZE}={self._batch_size} is invalid, assign '
                f'{self.DEFAULT_BATCH_SIZE} to query_entities_page_size"',
            )

    @staticmethod
    def _get_logger_prefix(task_config: dict, all_conf_contents: dict) -> str:
        storage_account_stanza_name = task_config[mscs_consts.ACCOUNT]
        storage_account_info = all_conf_contents[mscs_consts.ACCOUNTS][
            storage_account_stanza_name
        ]
        storage_account_name = storage_account_info[mscs_consts.ACCOUNT_NAME]
        pairs = [
            '{}="{}"'.format(k, v)
            for k, v in [
                (mscs_consts.STANZA_NAME, task_config.get(mscs_consts.STANZA_NAME)),
                (mscs_consts.ACCOUNT_NAME, storage_account_name),
                (
                    mscs_consts.CONTAINER_NAME,
                    task_config.get(mscs_consts.CONTAINER_NAME),
                ),
                (mscs_consts.BLOB_NAME, task_config.get(mscs_consts.BLOB_NAME)),
                (mscs_consts.BLOB_TYPE, task_config.get(mscs_consts.BLOB_TYPE)),
                (mscs_consts.BLOB_MODE, task_config.get(mscs_consts.BLOB_MODE)),
                (
                    mscs_consts.BLOB_COMPRESSION,
                    task_config.get(mscs_consts.BLOB_COMPRESSION),
                ),
                (mscs_consts.SNAPSHOT, task_config.get(mscs_consts.SNAPSHOT)),
            ]
            if v
        ]

        return "[{}]".format(" ".join(pairs))

    def _process_ckpt(self):
        ckpt_blob_creation_time = self._ckpt.get(mscs_consts.BLOB_CREATION_TIME, "")
        ckpt_received_bytes = self._ckpt.get(mscs_consts.RECEIVED_BYTES, 0)
        # New blob discovered, never consumed before
        if not self._ckpt.get(mscs_consts.LAST_MODIFIED):
            self._init_ckpt()
        # Doc reference for mode/types difference: https://shorturl.at/vHW39
        # all cases below assume that the BlobProperties.last_modified > ckpt_last_modified
        elif (
            self._blob_type == mss.BlobType.APPEND_BLOB
            or self._blob_mode == mss.BlobModeType.APPEND
            or self._snapshot
        ):
            if self._blob_creation_time > ckpt_blob_creation_time:
                """
                - BlobProperties.creation_time changed.
                    - When re-uploading from web console any blob with overwrite=True the BlobProperties.creation_time
                            DOES NOT change. The only way to support this inconsistency is by checking the size of the file.
                    - When re-uploading with SDK overwrite=True BlobProperties.creation_time DOES change as expected.
                """
                self._logger.warning(
                    f'message="Append blob|mode file re-uploaded." '
                    f"blob_size={self._blob_size}, "
                    f"blob_creation_time={self._blob_creation_time}, "
                    f"ckpt_blob_creation_time={ckpt_blob_creation_time}"
                )
            if self._should_process_reuploaded_blob(ckpt_received_bytes):
                """
                Append blob|mode
                Reset cases:
                - Amount of checkpointed bytes is higher than the scheduled blob.
                - Amount of checkpointed bytes equals to the scheduled blob AND `dont_reupload_blob_same_size is not set`.
                    This flag is needed to avoid duplicated events for blobs which are generated by Azure Diagnostics or
                    any other scenario where last_modified_date is changed but blob_size remains the same.
                """
                self._logger.warning(
                    f'message="Reset checkpoint. Blob shrunk in size." '
                    f"blob_size={self._blob_size}, "
                    f"ckpt_received_bytes={ckpt_received_bytes}"
                )
                self._init_ckpt()
            elif (
                ckpt_received_bytes == self._blob_size
                and self._dont_reupload_blob_same_size
            ):
                self._logger.debug(
                    "There was an attempt of reuploading the same blob as the "
                    "last_modified_date was updated and the blob_size didn't change. "
                    "It was denied by `dont_reupload_blob_same_size` flag being turned on."
                )
            else:
                self._ckpt[mscs_consts.IS_COMPLETED] = 0
                self._ckpt[mscs_consts.LAST_MODIFIED] = self._last_modified
                self._ckpt[mscs_consts.BLOB_CREATION_TIME] = self._blob_creation_time
        else:
            """
            default. Random mode. Always True.
                - Always download from zero bytes when last_modified is changed
            """
            ckpt_last_modified = self._ckpt.get(mscs_consts.LAST_MODIFIED, "")
            if self._last_modified > ckpt_last_modified:
                self._logger.info(
                    f"Reset checkpoint. The last_modified in checkpoint "
                    f"is not equal to the last_modified returned by blob_client, "
                    f"reinitialize the checkpoint."
                    f" blob_size={self._blob_size}"
                    f", last_modified={self._last_modified}"
                    f", ckpt_last_modified={ckpt_last_modified}"
                )
                self._init_ckpt()

    def _init_ckpt(self):
        self._ckpt[mscs_consts.RECEIVED_BYTES] = 0
        self._ckpt[mscs_consts.IS_COMPLETED] = 0
        self._ckpt[mscs_consts.LAST_MODIFIED] = self._last_modified
        self._ckpt[mscs_consts.BLOB_CREATION_TIME] = self._blob_creation_time
        self._ckpt[mscs_consts.CODESET] = None

    def _build_csv_events(self, content, is_finished):
        """Build Splunk events from processed CSV JSON content.

        Takes JSON content (one JSON object per line) and creates Splunk events
        for each line with proper source, sourcetype, and index metadata.

        Args:
            content (str): JSON content with one JSON object per line
            is_finished (bool): Whether this is the final chunk of data

        Returns:
            list: List of Splunk event objects
        """
        events = []
        total_bytes = 0
        if self._snapshot:
            source = ":".join((self._blob_name, self._snapshot))
        else:
            source = self._blob_name

        json_lines = [line.strip() for line in content.split("\n") if line.strip()]

        for json_line in json_lines:
            total_bytes += len(json_line.encode())
            event = dc.build_event(
                source=source,
                sourcetype=self._sourcetype,
                index=self._index,
                raw_data=json_line,
                is_unbroken=True,
                is_done=is_finished,
            )
            events.append(event)
        return events, total_bytes

    def _build_event(self, content, is_finished):
        if self._snapshot:
            source = ":".join((self._blob_name, self._snapshot))
        else:
            source = self._blob_name
        is_done = True if is_finished else False
        return dc.build_event(
            source=source,
            sourcetype=self._sourcetype,
            index=self._index,
            raw_data=content,
            is_unbroken=True,
            is_done=is_done,
        )

    @classmethod
    def _get_blob_len(cls, blob: StorageStreamDownloader):
        if not blob.properties.content_range:
            return 0
        index = blob.properties.content_range.find("/")
        return int(blob.properties.content_range[index + 1 :])  # noqa: E203

    def _should_process_reuploaded_blob(self, ckpt_received_bytes):
        return (ckpt_received_bytes > self._blob_size) or (
            ckpt_received_bytes == self._blob_size
            and not self._dont_reupload_blob_same_size
        )
