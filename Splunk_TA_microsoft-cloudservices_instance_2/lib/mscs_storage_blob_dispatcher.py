#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
import dataclasses
import json
import re
import time
import random
import copy
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

from azure.storage.blob import BlobProperties

import mscs_util

import mscs_checkpoint_util
import mscs_consts
import mscs_storage_blob_data_collector
import mscs_storage_dispatcher
import mscs_storage_service
from solnlib import utils

from mscs_storage_dispatcher import (
    BlobKeyError,
    BlobKeyBusy,
    BlobKeyNotFound,
    BlobKeyNotUpdated,
)


@dataclasses.dataclass
class ScanStats:
    total_scanned: int = 0
    filter_matched: int = 0
    checkpointer_matched: int = 0

    checkpointer_get: int = 0
    checkpointer_update: int = 0
    checkpointer_batch_save: int = 0


PAGE_SIZE = 5000


class StorageBlobDispatcher(mscs_storage_dispatcher.StorageDispatcher):
    def __init__(
        self, all_conf_contents, meta_config, task_config, data_writer, logger
    ):
        super(StorageBlobDispatcher, self).__init__(
            all_conf_contents, meta_config, task_config, data_writer, logger, True
        )
        self._container_name = self._task_config[mscs_consts.CONTAINER_NAME]
        self._include_snapshots = utils.is_true(
            self._task_config.get(mscs_consts.INCLUDE_SNAPSHOTS)
        )
        self._snapshots_start_time = self._task_config.get(
            mscs_consts.SNAPSHOTS_START_TIME
        )
        self._snapshots_end_time = self._task_config.get(mscs_consts.SNAPSHOTS_END_TIME)
        self._storage_service = mscs_storage_service.BlobStorageService(
            all_conf_contents=all_conf_contents,
            meta_config=meta_config,
            task_config=task_config,
            proxy_config=self._proxy_config,
            storage_account_config=self._storage_account_config,
            logger=logger,
        )

        self._checkpoint_dir = self.get_checkpoint_dir()
        self._scan_stats = ScanStats()

    def _log_stats(self):
        if self._scan_stats:
            self._logger.info(f"collection_stats {self._scan_stats}")
            self._scan_stats = ScanStats()

    def _get_patterns(self):
        blob_list_str = self._task_config.get(mscs_consts.BLOB_LIST)
        if blob_list_str is None or not blob_list_str.strip():
            patterns = {".*": 3}
        else:
            patterns = self._get_blob_patterns(blob_list_str)
        self._logger.debug("The patterns = %s", patterns)
        return patterns

    def _get_application_insights(self):
        application_insights = self._task_config.get(mscs_consts.APPLICATION_INSIGHTS)
        self._logger.info(
            "The application insights checkbox value is = %s", application_insights
        )
        return str(application_insights)

    def _get_application_log_prefixes(self):
        log_type = self._task_config.get(mscs_consts.LOG_TYPE)
        guids = self._task_config.get(mscs_consts.GUIDS)
        self._logger.debug("The log type = %s, The GUIDS are = %s" % (log_type, guids))
        prefixes = []
        if guids and log_type:
            data = guids.split(",")
            for guid in data:
                prefix = guid + "/" + log_type
                prefixes.append(prefix)
        return prefixes

    def _get_exclude_patterns(self):
        exclude_blob_list_str = self._task_config.get(mscs_consts.EXCLUDE_BLOB_LIST)
        if exclude_blob_list_str is None or not exclude_blob_list_str.strip():
            exclude_patterns = {}
        else:
            exclude_patterns = self._get_blob_patterns(exclude_blob_list_str)
        self._logger.debug("The exclude patterns = %s", exclude_patterns)
        return exclude_patterns

    def _get_blob_patterns(self, blob_list_str):
        if not blob_list_str:
            return {}
        try:
            pattern_dct = json.loads(blob_list_str)
        except ValueError:
            blob_pattern_lst = [
                blob.strip() for blob in blob_list_str.split(",") if len(blob.strip())
            ]
            pattern_dct = {}
            for blob_pattern in blob_pattern_lst:
                if blob_pattern.find("*") != -1:
                    pattern_dct[blob_pattern] = 2
                else:
                    pattern_dct[blob_pattern] = 1

        processed_pattern_dct = {}
        for k, v in pattern_dct.items():
            if v == 1:
                processed_pattern_dct[k] = v
            else:
                if v == 2:
                    k2 = k.replace("*", ".*") + "$"
                else:
                    k2 = k + "$"
                try:
                    re.compile(k2)
                except re.error as e:
                    self._logger.warning(f'message="{e}, blob is invalid", blob={k}')
                    continue
                processed_pattern_dct[k2] = 3
        return processed_pattern_dct

    def _gen_prefixes(self):
        application_insights = self._get_application_insights()
        app_log_prefixes = self._get_application_log_prefixes()
        prefix_list = []
        # Get the prefix from the UI
        prefix = self._task_config.get(mscs_consts.PREFIX)
        if prefix:
            prefix_list.append(prefix)

        if application_insights == "1":
            self._logger.debug(
                "Application insights is 1. App log prefixes are : %s", app_log_prefixes
            )
            now = datetime.now(timezone.utc)
            current_time = now.strftime("%Y-%m-%d/%H")
            before_time = (now - timedelta(hours=1)).strftime("%Y-%m-%d/%H")
            for app_log_prefix in app_log_prefixes:
                current_prefix = app_log_prefix + "/" + current_time
                before_prefix = app_log_prefix + "/" + before_time
                prefix_list.extend([current_prefix, before_prefix])

        if not prefix_list:
            prefix_list.append(None)

        self._logger.debug("All provided prefixes are : %s", prefix_list)
        return prefix_list

    @mscs_util.log_time_of_execution
    def _confirm_checkpoint_lock(
        self, expected_ckpt: dict, last_seen_lock_id: int, storage_info: dict
    ) -> bool:
        """
        _confirm_checkpoint_lock

        double checks if the key in kv store is still locked by
        this process.

        Arguments:
            expected_ckpt
            storage_info
            last_seen_lock_id


        Raises:
            mscs_storage_dispatcher.BlobKeyBusy: if already used by other process
        """
        blob_name = storage_info["blob_name"]
        current_ckpt = self._get_ckpt(blob_name, snapshot=storage_info["snapshot"])
        self._logger.debug(
            f"confirming lock and lock_id for blob={blob_name}, current_ckpt={current_ckpt} expected_ckpt={expected_ckpt}"
        )
        if not current_ckpt:
            self._logger.warning(
                f'message="Couldnt find checkpoint" '
                f"blob={blob_name} while confirming lock"
            )
            raise BlobKeyNotFound(blob_name=blob_name)
        elif current_ckpt["lock_id"] == last_seen_lock_id:
            self._logger.warning(
                f'message="Checkpoint was not updated" '
                f"blob={blob_name}, current_ckpt={current_ckpt}, "
                f"last_seen_lock_id={last_seen_lock_id}"
            )
            raise BlobKeyNotUpdated(blob_name)
        elif current_ckpt["lock_id"] != expected_ckpt["lock_id"]:
            self._logger.warning(
                f'message="Blob busy with other input" ' f"blob_name={blob_name}"
            )
            raise BlobKeyBusy(blob_name)
        return True

    def _get_shuffled_buffer(
        self, blob_list: Iterable[BlobProperties], buffer_size: int
    ) -> Iterable[BlobProperties]:
        """
        shuffling with a buffer lowers the chances of checkpoint lock clashes
        """
        buffer = []
        for blob in blob_list:
            buffer.append(blob)
            if len(buffer) >= buffer_size:
                random.shuffle(buffer)
                for b in buffer:
                    yield b
                buffer = []

        random.shuffle(buffer)
        for b in buffer:
            yield b

    def _get_checkpoint_name_and_key(self, storage_info):
        checkpoint_name = mscs_checkpoint_util.get_blob_checkpoint_name(
            self._container_name,
            storage_info["name"],
            storage_info["snapshot"],
        )
        key = self._checkpointer.format_key(checkpoint_name)
        return checkpoint_name, key

    @mscs_util.log_time_of_execution
    def _do_batch_checkpoint(self, batch_checkpoint):
        """
        Do batch call to update the checkpoint
        :param batch_checkpoint: dict of various checkpoint with key and value
        """
        if not batch_checkpoint:
            return
        ckpt_list = list(batch_checkpoint.values())
        self._checkpointer.batch_save(ckpt_list)
        self._scan_stats.checkpointer_batch_save += 1
        batch_checkpoint.clear()

    def _cancel_dispatch(self, sub_task_config_list):
        """
        Cancel dispatching and update the checkpoint lock to 0 for the blobs which acquired lock
        :param sub_task_config_list: list of sub task config
        """
        batch_checkpoint = {}
        for stc in sub_task_config_list:
            ckpt_key = mscs_checkpoint_util.get_blob_checkpoint_name(
                self._container_name, stc["blob_name"], stc["snapshot"]
            )
            checkpoint = self._get_ckpt(
                blob_name=stc["blob_name"], snapshot=stc["snapshot"]
            )
            if not checkpoint:
                continue
            checkpoint["lock"] = 0
            batch_checkpoint[ckpt_key] = self._checkpointer.get_formatted_record(
                ckpt_key, checkpoint
            )
        self._do_batch_checkpoint(batch_checkpoint)

    def _do_dispatch(self):
        try:
            super()._do_dispatch()
        finally:
            self._log_stats()

    def _dispatch_tasks(self, patterns):
        # sleep so that on start inputs do not try to read same file at the same time
        # this is only a problem is there is only one or two files in the containor
        time.sleep(random.randint(1, 10))

        for prefix in self._gen_prefixes():
            # storage_name_set = set()
            try:
                storage_info_lst = self._get_storage_info_list(patterns, prefix)
            except Exception as e:
                self._logger.error(
                    f'message="Exception occurred while fetching list of blobs: {e}"',
                    exc_info=e,
                )
                continue

            task_futures = []
            sub_task_config_list = []
            process_blob_count = 0

            for storage_info in self._get_shuffled_buffer(storage_info_lst, PAGE_SIZE):
                try:
                    ckpt = self._get_ckpt(storage_info.name, storage_info.snapshot)

                    sub_task_config = self._get_sub_task_config(storage_info, ckpt)
                    if not sub_task_config:
                        continue
                    process_blob_count += 1
                    sub_task_config_list.append(sub_task_config)
                except BlobKeyError as e:
                    self._logger.warning(
                        f'message="Unsupported blob name, it contains some non-ASCII characters" '
                        f"blob={e.blob_name}"
                    )
                    continue
                except BlobKeyBusy as e:
                    self._logger.debug(
                        "Blob busy with other input - blob=%s",
                        e.blob_name,
                    )
                    continue
                except Exception as e:
                    self._logger.warning(
                        f'message="Blob with unknown checkpoint exception: {e}" '
                        f"blob={storage_info.name}",
                        exc_info=e,
                    )
                    continue

                # confirm ckpt lock after updating ckpt for 100 blobs
                # As KV Store does not have "read after write" consistency but offers eventual consistency
                if len(sub_task_config_list) < mscs_consts.CHUNK_SIZE:
                    continue

                self._logger.info(
                    "The number of blobs to be processed: %d", len(sub_task_config_list)
                )

                # Retrive the ckpt and add lock
                process_blobs = []
                for stc in sub_task_config_list:
                    blob_info = self._get_and_lock_ckpt(stc)
                    if blob_info:
                        process_blobs.append(blob_info)

                    if self._canceled.is_set():
                        return
                for p_blob in process_blobs:
                    task_future = self._dispatch(
                        ckpt=p_blob["ckpt"],
                        sub_task_config=p_blob["sub_task_config"],
                        last_seen_lock_id=p_blob["last_seen_lock_id"],
                    )
                    if not task_future:
                        continue
                    task_futures.append(task_future)
                    task_futures = self._wait_while_full(
                        task_futures, self._worker_threads_num
                    )
                    if self._canceled.is_set():
                        self._cancel_dispatch(sub_task_config_list)

                    if self._cancel_sub_tasks(self._canceled):
                        return
                sub_task_config_list = []

            if len(sub_task_config_list):
                self._logger.info(
                    "The number of blobs to be processed: %d", len(sub_task_config_list)
                )

                if process_blob_count < mscs_consts.CHUNK_SIZE:
                    self._logger.debug("Processing blobs without chunking")

                    for sub_task_config in sub_task_config_list:
                        blob_info = self._get_and_lock_ckpt(sub_task_config)

                        if not blob_info:
                            continue

                        task_future = self._dispatch(
                            ckpt=blob_info["ckpt"],
                            sub_task_config=sub_task_config,
                            last_seen_lock_id=blob_info["last_seen_lock_id"],
                        )
                        if not task_future:
                            continue
                        task_futures.append(task_future)
                        task_futures = self._wait_while_full(
                            task_futures, self._worker_threads_num
                        )
                        if self._canceled.is_set():
                            self._cancel_dispatch(sub_task_config_list)

                        if self._cancel_sub_tasks(self._canceled):
                            return
                else:
                    process_blobs = []
                    for stc in sub_task_config_list:
                        blob_info = self._get_and_lock_ckpt(stc)
                        if blob_info:
                            process_blobs.append(blob_info)

                        if self._canceled.is_set():
                            return

                    for p_blob in process_blobs:
                        task_future = self._dispatch(
                            ckpt=p_blob["ckpt"],
                            sub_task_config=p_blob["sub_task_config"],
                            last_seen_lock_id=p_blob["last_seen_lock_id"],
                        )
                        if not task_future:
                            continue
                        task_futures.append(task_future)
                        task_futures = self._wait_while_full(
                            task_futures, self._worker_threads_num
                        )
                        if self._canceled.is_set():
                            self._cancel_dispatch(sub_task_config_list)

                        if self._cancel_sub_tasks(self._canceled):
                            return

            sub_task_config_list = []
            self._logger.info("Total number of blobs processed: %d", process_blob_count)
            self._wait_fs(task_futures)
        self._executor.shutdown(wait=not self._canceled.is_set())

    def _dispatch(self, ckpt, sub_task_config, last_seen_lock_id):
        """
        Dispatch the task to collect the data for the blob
        :param ckpt: checkpoint details
        :param sub_task_config: blob details
        """
        running_task = self._get_running_task()

        task_future = self._executor.submit(
            running_task,
            self._all_conf_contents,
            self._meta_config,
            sub_task_config,
            ckpt,
            last_seen_lock_id,
            self._canceled,
            self._data_writer,
            self._logger,
            self._confirm_checkpoint_lock,
            self._checkpointer,
            self._proxy_config,
            self._storage_account_config,
        )
        return task_future

    def _get_storage_info_list(self, patterns, prefix=None) -> Iterable[BlobProperties]:
        """
        Returns the qualified blob iterator for the blobs under the specified container.
        :param str patterns:
            Indicates the patterns for the data in the blob container.
        :param str prefix:
            Filters the results to return only blobs whose names
            begin with the specified prefix.
        """

        container_client = self._storage_service.get_container_client()
        # If snapshots are included in the StorageDispatcher. or go to else construct
        if self._include_snapshots:
            blobs = container_client.list_blobs(
                include=mscs_consts.SNAPSHOT,
                name_starts_with=prefix,
                results_per_page=PAGE_SIZE,
            )
        else:
            blobs = container_client.list_blobs(
                name_starts_with=prefix, results_per_page=PAGE_SIZE
            )

        return self.blob_generator(blobs, patterns)

    # src_blob_lst = [blob for blob in blobs]

    def blob_generator(self, blobs: Iterable[BlobProperties], patterns):
        # self._logger.debug(
        #     "The number of blobs in container %s is %d",
        #     str(self._container_name),
        #     len(src_blob_lst),
        # )

        exclude_patterns = self._get_exclude_patterns()
        # Appending the blobs to blob_list based on what is_match() returns with patterns
        for blob in blobs:
            self._scan_stats.total_scanned += 1
            if (
                blob.size > 0
                and self._is_match(
                    blob, patterns, self._snapshots_start_time, self._snapshots_end_time
                )
                and not self._is_match(
                    blob,
                    exclude_patterns,
                    self._snapshots_start_time,
                    self._snapshots_end_time,
                )
            ):
                self._scan_stats.filter_matched += 1
                yield blob

    def _get_and_lock_ckpt(self, sub_task_config) -> Optional[dict]:
        """Used to get a checkpoint from kv store and confirm the lock with latest details.

        Args:
            sub_task_config (dict): blob information

        Raises:
            BlobKeyError: raised if blob key error
            BlobKeyBusy: raised if blob is busy

        Returns:
            dict: dictionary of latest checkpoint details and sub_task_config
        """
        try:
            latest_ckpt = self._get_ckpt(
                blob_name=sub_task_config[mscs_consts.BLOB_NAME],
                snapshot=sub_task_config[mscs_consts.SNAPSHOT],
            )

            if not self._should_process_blob(
                ckpt=latest_ckpt, sub_task_config=sub_task_config
            ):
                return None

            new_checkpoint = self._lock_ckpt(
                ckpt=latest_ckpt,
                sub_task_config=sub_task_config,
            )
            return {
                "last_seen_lock_id": latest_ckpt.get("lock_id"),
                "ckpt": new_checkpoint,
                "sub_task_config": sub_task_config,
            }

        except BlobKeyError as e:
            self._logger.warning(
                f'message="Unsupported blob name, it contains some non-ASCII characters" '
                f"blob={sub_task_config[mscs_consts.BLOB_NAME]}",
                exc_info=e,
            )
        except BlobKeyBusy:
            self._logger.debug(
                f"Blob busy with other input - blob={sub_task_config[mscs_consts.BLOB_NAME]}"
            )
        except Exception as e:
            self._logger.warning(
                f'message="Blob with unknown checkpoint exception: {e}" '
                f"blob={sub_task_config[mscs_consts.BLOB_NAME]}",
                exc_info=e,
            )

    @mscs_util.log_time_of_execution
    def _get_ckpt(self, blob_name: str, snapshot: str) -> dict:
        """Used to get a checkpoint from kv store.

        Args:
            storage_info (dict): blob information

        Raises:
            mscs_storage_dispatcher.BlobKeyError: raised if blob key error

        Returns:
            dict: checkpoint dictionary from kv store
        """
        self._scan_stats.checkpointer_get += 1
        try:
            checkpoint_name = mscs_checkpoint_util.get_blob_checkpoint_name(
                self._container_name, blob_name, snapshot
            )
            return self._checkpointer.get(checkpoint_name) or {}
        except KeyError:
            raise mscs_storage_dispatcher.BlobKeyError(checkpoint_name)

    def _lock_ckpt(self, ckpt: dict, sub_task_config: dict) -> dict:
        """Used to lock the checkpoint and update it into kv store.

        Args:
            ckpt (dict): checkpoint dictionary
            sub_task_config (dict): latest information of the received blob

        """
        blob_name = sub_task_config[mscs_consts.BLOB_NAME]
        snapshot = sub_task_config[mscs_consts.SNAPSHOT]
        checkpoint_name = mscs_checkpoint_util.get_blob_checkpoint_name(
            self._container_name, blob_name, snapshot
        )
        try:
            current_lock_time = ckpt.get("lock", 0)
            now = time.time()
            if current_lock_time > now:
                raise BlobKeyBusy(blob_name=blob_name)
            new_checkpoint = copy.deepcopy(ckpt)

            if (
                sub_task_config.get(mscs_consts.BLOB_TYPE)
                == mscs_storage_service.BlobType.APPEND_BLOB
                or sub_task_config.get(mscs_consts.BLOB_MODE)
                == mscs_storage_service.BlobModeType.APPEND
            ):
                lock_time = mscs_consts.BLOB_SCHEDULER_BLOCK_TIME_APPEND
            else:
                lock_time = mscs_consts.BLOB_SCHEDULER_BLOCK_TIME
            new_checkpoint["lock"] = time.time() + lock_time
            new_checkpoint["lock_id"] = random.randint(1000, 99999999999)
            self._checkpointer.update(checkpoint_name, new_checkpoint)

            self._scan_stats.checkpointer_update += 1
            self._logger.debug(
                f"checkpoint updated"
                f' lock={new_checkpoint["lock"]}'
                f' lock_id={new_checkpoint["lock_id"]}'
                f" lock_time={lock_time}"
            )
            return new_checkpoint
        except KeyError:
            raise mscs_storage_dispatcher.BlobKeyError(checkpoint_name)

    def _should_process_blob(self, ckpt: dict, sub_task_config: dict) -> bool:
        ckpt = ckpt if ckpt else {}
        is_completed = utils.is_true(ckpt.get(mscs_consts.IS_COMPLETED))
        ckpt_last_modified = ckpt.get(mscs_consts.LAST_MODIFIED, "")
        if (
            is_completed
            and ckpt_last_modified
            and ckpt_last_modified >= sub_task_config[mscs_consts.LAST_MODIFIED]
        ):
            return False
        self._scan_stats.checkpointer_matched += 1
        return True

    def _get_sub_task_config(self, storage_info: BlobProperties, ckpt: dict):
        sub_task_config = {}
        sub_task_config[mscs_consts.BLOB_NAME] = storage_info.name
        sub_task_config[mscs_consts.SNAPSHOT] = storage_info.snapshot
        sub_task_config[mscs_consts.BLOB_TYPE] = storage_info.blob_type

        # ContainerClient.list_blobs returns eTAG wrapped in ", i.e. etag = '"0x8DC526EC63F2739"'
        sub_task_config[mscs_consts.ETAG] = storage_info.etag.replace('"', "")
        sub_task_config[
            mscs_consts.LAST_MODIFIED
        ] = storage_info.last_modified.isoformat("T")
        sub_task_config[
            mscs_consts.BLOB_CREATION_TIME
        ] = storage_info.creation_time.isoformat("T")
        sub_task_config[mscs_consts.BLOB_SIZE] = storage_info.size

        if self._should_process_blob(ckpt=ckpt, sub_task_config=sub_task_config):
            self._logger.debug(f"should_process_blob={sub_task_config}")

            return {**self._task_config, **sub_task_config}
        return None

    def _get_running_task(self):
        return mscs_storage_blob_data_collector.running_task

    @classmethod
    def _is_match(
        cls, storage_info, pattern_dct, snapshots_start_time, snapshots_end_time
    ):
        if storage_info.snapshot:
            if snapshots_start_time and storage_info.snapshot < snapshots_start_time:
                return False
            if snapshots_end_time and storage_info.snapshot > snapshots_end_time:
                return False
        for k, v in pattern_dct.items():
            if v == 1:
                if storage_info.name == k:
                    return True
            elif v == 3:
                if re.match(k, storage_info.name):
                    return True
        return False
