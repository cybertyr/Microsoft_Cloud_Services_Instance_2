#!/usr/bin/python
#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
import datetime
import json
import time
from uuid import UUID

import mscs_consts
import mscs_logger as logger
import mscs_storage_service as mss
import mscs_util
import splunktaucclib.common.log as stulog  # pylint: disable=import-error
import splunktaucclib.data_collection.ta_data_client as dc  # pylint: disable=import-error
import table_collector_helper as tchelper
from azure.data.tables import EntityProperty  # pylint: disable=import-error
from solnlib.observability import StanzaObservabilityRecorder

from splunk_ta_mscs.models import ProxyConfig, AzureStorageAccountConfig


def running_task(
    all_conf_contents,
    meta_config,
    task_config,
    ckpt,
    canceled,
    data_writer,
    logger_prefix,
    proxy_config: ProxyConfig,
    storage_account_config: AzureStorageAccountConfig,
):
    try:
        data_collector = StorageTableDataCollector(
            all_conf_contents,
            meta_config,
            task_config,
            ckpt,
            canceled,
            data_writer,
            proxy_config,
            storage_account_config,
        )
        data_collector.collect_data()
    except Exception as e:
        stulog.logger.exception(
            f'{logger_prefix} message="Exception@running_task(): {e}"'
        )


# Custom encoder for type: bytes
class _ExtendedEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return "<<non-serializable: bytes>>"
        return json.JSONEncoder.default(self, obj)


class StorageTableDataCollector(mss.TableStorageService):
    MAX_PAGE_SIZE = 1000

    _partition_key = "PartitionKey"
    _row_key = "RowKey"
    _timestamp = "Timestamp"

    def __init__(
        self,
        all_conf_contents,
        meta_config,
        task_config,
        ckpt,
        canceled,
        data_writer,
        proxy_config,
        storage_account_config: AzureStorageAccountConfig,
    ):
        super().__init__(
            all_conf_contents=all_conf_contents,
            meta_config=meta_config,
            task_config=task_config,
            proxy_config=proxy_config,
            storage_account_config=storage_account_config,
            logger=logger.logger_for(
                self._get_logger_prefix(
                    task_config=task_config, all_conf_contents=all_conf_contents
                ),
            ),
        )
        self._ckpt = ckpt if ckpt else {}
        self._canceled = canceled
        self._data_writer = data_writer
        self._table_name = None
        self._start_time = None
        self._index = None
        self._sourcetype = None
        self._query_entities_page_size = None
        self._event_cnt_per_item = None
        self._query_end_time_offset = None
        self._init_from_task_config()
        stanza_name = self._task_config[mscs_consts.STANZA_NAME]
        self._obs = StanzaObservabilityRecorder(
            "storage-table", self._logger, stanza_name
        )

    def collect_data(self):
        try:
            if self._canceled.is_set():
                self._logger.info("The task is canceled.")
                return
            self._logger.info("Starting to collect data.")

            with self._obs:
                self._do_collect_data()

                self._logger.info("Collecting data finished.")
        except Exception as e:
            self._logger.exception(
                'message="Error occurred in collecting data"', exc_info=e
            )

    def _do_collect_data(self):
        self._logger.info("Starting to pre-process checkpoint.")
        self._pre_process_ckpt()

        self._logger.info("Pre-process checkpoint finished.")
        # check the canceled flag
        if self._canceled.is_set():
            return
        # get table client
        table_client = self.get_table_client(self._table_name)
        # generate filter string
        filter_string = self._generate_filter_string()

        self._logger.info('The filter is "%s".', filter_string)

        self._logger.info(
            "query_entities_page_size=%s event_cnt_per_item=%s query_end_time_offset=%s",
            self._query_entities_page_size,
            self._event_cnt_per_item,
            self._query_end_time_offset,
        )

        num_page = 1

        while not self._canceled.is_set():
            ckpt_status = self._ckpt[mscs_consts.STATUS]
            self._logger.debug("The current checkpoint status=%s", ckpt_status)

            if ckpt_status == mscs_consts.CheckpointStatusType.ALL_DONE:
                self._logger.info(
                    "%s All entities are collected in current timestamp range."
                )
                break

            self._logger.debug(
                "Querying entities for table name=%s filter=%s page=%s started",
                self._table_name,
                filter_string,
                num_page,
            )
            before = time.time()

            entities = table_client.query_entities(
                query_filter=filter_string,
                results_per_page=self._query_entities_page_size,
            )

            self._logger.debug(
                "Querying entities for table name=%s filter=%s page=%s "
                "finished. Cost %s seconds",
                self._table_name,
                filter_string,
                num_page,
                time.time() - before,
            )
            self._process_entities(entities)

            num_page += 1

    def _init_from_task_config(self):
        self._table_name = self._task_config[mscs_consts.TABLE_NAME]
        self._start_time = self._task_config[mscs_consts.START_TIME]
        self._index = self._task_config[mscs_consts.INDEX]
        self._sourcetype = self._task_config[mscs_consts.SOURCETYPE]

        global_settings = self._all_conf_contents[mscs_consts.GLOBAL_SETTINGS]
        tuning_settings = global_settings[mscs_consts.PERFORMANCE_TUNING_SETTINGS]

        self._query_entities_page_size = int(
            mscs_util.find_config_in_settings(
                mscs_consts.QUERY_ENTITIES_PAGE_SIZE,
                mscs_consts.QUERY_ENTITIES_PAGE_SIZE_DEFAULT_VALUE,
                self._task_config,
                tuning_settings,
            )
        )

        if (
            self._query_entities_page_size <= 0
            or self._query_entities_page_size > self.MAX_PAGE_SIZE
        ):
            raise Exception(
                "{}={} is invalid".format(
                    mscs_consts.QUERY_ENTITIES_PAGE_SIZE, self._query_entities_page_size
                )
            )

        self._event_cnt_per_item = int(
            mscs_util.find_config_in_settings(
                mscs_consts.EVENT_CNT_PER_ITEM,
                mscs_consts.EVENT_CNT_PER_ITEM_DEVAULT_VALUE,
                self._task_config,
                tuning_settings,
            )
        )
        self._query_end_time_offset = float(
            mscs_util.find_config_in_settings(
                mscs_consts.QUERY_END_TIME_OFFSET,
                mscs_consts.QUERY_END_TIME_OFFSET_DEFAULT_VALUE,
                self._task_config,
                tuning_settings,
            )
        )

    @staticmethod
    def _get_logger_prefix(task_config: dict, all_conf_contents: dict) -> str:
        storage_account_stanza_name = task_config[mscs_consts.ACCOUNT]
        storage_account_info = all_conf_contents[mscs_consts.ACCOUNTS][
            storage_account_stanza_name
        ]
        storage_account_name = storage_account_info[mscs_consts.ACCOUNT_NAME]
        pairs = [
            '{}="{}"'.format(
                mscs_consts.STANZA_NAME, task_config[mscs_consts.STANZA_NAME]
            ),
            '{}="{}"'.format(mscs_consts.ACCOUNT_NAME, storage_account_name),
            '{}="{}"'.format(
                mscs_consts.TABLE_NAME, task_config[mscs_consts.TABLE_NAME]
            ),
        ]
        return "[{}]".format(" ".join(pairs))

    def _pre_process_ckpt(self):
        ckpt_start_time = self._ckpt.get(mscs_consts.START_TIME)
        query_end_time = mscs_util.timestamp_to_utc(
            time.time() - self._query_end_time_offset
        )

        # When this is the first time to collect data or
        # user modified the start time in conf
        if self._start_time != ckpt_start_time:
            self._logger.info(
                "The start time in conf is not equal to the start time in checkpoint, "
                "reinitialize the checkpoint."
            )

            self._init_ckpt(query_end_time)
        else:
            ckpt_status = self._ckpt[mscs_consts.STATUS]

            self._logger.info("The checkpoint status=%s.", ckpt_status)

            if ckpt_status == mscs_consts.CheckpointStatusType.ALL_DONE:
                self._process_all_done_ckpt(query_end_time)
            elif ckpt_status == mscs_consts.CheckpointStatusType.CUR_PAGE_DONE:
                self._logger.info(
                    "Continue to collect entities after page=%s.",
                    self._ckpt[mscs_consts.PAGE_LINK],
                )
            elif ckpt_status == mscs_consts.CheckpointStatusType.CUR_PAGE_ONGOING:
                self._logger.info(
                    "Continue to collect entities for page=%s.",
                    self._ckpt[mscs_consts.PAGE_LINK],
                )
            else:
                raise Exception(
                    "The checkpoint status={} is invalid.".format(ckpt_status)
                )
        # write the ckpt
        self._data_writer.write_ckpt(self._table_name, self._ckpt, self._canceled)

    def _init_ckpt(self, query_end_time):
        self._ckpt[mscs_consts.START_TIME] = self._start_time
        self._ckpt[mscs_consts.QUERY_START_TIME] = self._start_time
        self._ckpt[mscs_consts.QUERY_END_TIME] = query_end_time
        self._ckpt[mscs_consts.PAGE_LINK] = None
        self._ckpt[mscs_consts.CUR_PARTITIONKEY] = None
        self._ckpt[mscs_consts.CUR_ROWKEY] = None
        self._ckpt[mscs_consts.CUR_TIMESTAMP] = None
        self._ckpt[mscs_consts.STATUS] = mscs_consts.CheckpointStatusType.CUR_PAGE_DONE

    def _process_all_done_ckpt(self, query_end_time):
        last_end_time = self._ckpt[mscs_consts.QUERY_END_TIME]
        last_start_time = self._ckpt[mscs_consts.QUERY_START_TIME]

        msg = (
            "The entities in range (%s, %s] are all collected, updating the checkpoint."
        )
        if tchelper.is_websitesapp_table(self._table_name):
            # For websitesapp*, if nothing found in last round, do not
            # update query_start_time. Otherwise set the query_start_time as
            # the timestamp of last entity.
            if mscs_consts.CUR_TIMESTAMP in self._ckpt:
                last_timestamp = self._ckpt.get(mscs_consts.CUR_TIMESTAMP)
            else:
                # There is no cur_timestamp in ckpt if migrated from
                #  previous releases
                last_timestamp = last_end_time
            if last_timestamp:
                self._logger.info(msg, last_start_time, last_timestamp)
                self._ckpt[mscs_consts.QUERY_START_TIME] = last_timestamp
            else:
                self._logger.info(
                    "No entities found in range (%s, %s], will collect again.",
                    last_start_time,
                    last_end_time,
                )
        else:
            self._logger.info(msg, last_start_time, last_end_time)
            self._ckpt[mscs_consts.QUERY_START_TIME] = last_end_time

        # jscpd:ignore-start
        self._ckpt[mscs_consts.QUERY_END_TIME] = query_end_time
        self._ckpt[mscs_consts.PAGE_LINK] = None
        self._ckpt[mscs_consts.CUR_PARTITIONKEY] = None
        self._ckpt[mscs_consts.CUR_ROWKEY] = None
        self._ckpt[mscs_consts.CUR_TIMESTAMP] = None
        self._ckpt[mscs_consts.STATUS] = mscs_consts.CheckpointStatusType.CUR_PAGE_DONE
        # jscpd:ignore-end

    def _assemble_filter(self, start_time, end_time):
        query_terms = []
        from_partition, to_partition = tchelper.generate_partition_key(
            self._table_name, start_time, end_time
        )
        self._logger.debug(
            f"table={self._table_name} start_time={start_time} from_partition={from_partition} end_time={end_time} to_partition={to_partition}"
        )
        if from_partition is not None:
            query_terms.append("(PartitionKey ge '{}')".format(from_partition))
        if to_partition is not None:
            query_terms.append("(PartitionKey le '{}')".format(to_partition))
        query_terms.append(
            "(Timestamp gt datetime'{}') and (Timestamp le datetime'{}')".format(
                start_time, end_time
            )
        )
        return " and ".join(query_terms)

    def _generate_filter_string(self):
        query_start_time = self._ckpt.get(mscs_consts.QUERY_START_TIME)
        query_end_time = self._ckpt.get(mscs_consts.QUERY_END_TIME)
        return self._assemble_filter(
            start_time=query_start_time,
            end_time=query_end_time,
        )

    def _process_entities(self, entities):
        entity_lst = []
        num_entities, indexed = 0, 0
        max_timestamp = self._ckpt[mscs_consts.CUR_TIMESTAMP]

        for entity in entities:
            if self._canceled.is_set():
                self._logger.debug(
                    "Processed %s entities, indexed %s events", num_entities, indexed
                )
                return

            entity = self._convert_entity(entity)
            num_entities += 1

            if len(entity_lst) >= self._event_cnt_per_item:
                events, event_bytes = self._convert_entities_to_events(entity_lst)
                self._ckpt[mscs_consts.CUR_PARTITIONKEY] = entity_lst[-1][
                    self._partition_key
                ]
                self._ckpt[mscs_consts.CUR_ROWKEY] = entity_lst[-1][self._row_key]
                self._ckpt[mscs_consts.CUR_TIMESTAMP] = max_timestamp
                self._ckpt[
                    mscs_consts.STATUS
                ] = mscs_consts.CheckpointStatusType.CUR_PAGE_ONGOING

                indexed += len(entity_lst)
                self._data_writer.write_events_and_ckpt(
                    events, self._table_name, self._ckpt, self._canceled
                )
                self._obs.record(len(entity_lst), event_bytes)
                entity_lst = []

            if self._is_meet_requirement(entity, self._ckpt):
                entity_lst.append(entity)
                max_timestamp = (
                    max(max_timestamp, entity[self._timestamp])
                    if max_timestamp
                    else entity[self._timestamp]
                )

        self._ckpt[mscs_consts.PAGE_LINK] = None
        self._ckpt[mscs_consts.CUR_TIMESTAMP] = max_timestamp
        if len(entity_lst) > 0:
            self._ckpt[mscs_consts.CUR_PARTITIONKEY] = entity_lst[-1][
                self._partition_key
            ]
            self._ckpt[mscs_consts.CUR_ROWKEY] = entity_lst[-1][self._row_key]

        self._ckpt[mscs_consts.STATUS] = mscs_consts.CheckpointStatusType.ALL_DONE

        events, event_bytes = self._convert_entities_to_events(entity_lst)

        self._data_writer.write_events_and_ckpt(
            events, self._table_name, self._ckpt, self._canceled
        )
        indexed += len(events)
        self._obs.record(len(entity_lst), event_bytes)

        self._logger.debug(
            "Processed %s entities, indexed %s events", num_entities, indexed
        )
        return

    @classmethod
    def _is_meet_requirement(cls, entity, ckpt):
        if (ckpt[mscs_consts.CUR_PARTITIONKEY] is None) and (
            ckpt[mscs_consts.CUR_ROWKEY] is None
        ):
            return True
        return entity[cls._partition_key] > ckpt[mscs_consts.CUR_PARTITIONKEY] or (
            entity[cls._partition_key] == ckpt[mscs_consts.CUR_PARTITIONKEY]
            and entity[cls._row_key] > ckpt[mscs_consts.CUR_ROWKEY]
        )

    @staticmethod
    def _source_for(account_name, table_name):
        return "{account_name}://{table_name}".format(
            account_name=account_name, table_name=table_name
        )

    def _convert_entities_to_events(self, entities):
        events = []
        total_bytes = 0
        source = self._source_for(self._storage_account_config.name, self._table_name)

        for entity in entities:
            entity = self._process_vm_metrics_table(entity)
            raw = json.dumps(entity, cls=_ExtendedEncoder)
            total_bytes += len(raw.encode())
            events.append(
                dc.build_event(
                    source=source,
                    sourcetype=self._sourcetype,
                    index=self._index,
                    raw_data=raw,
                )
            )
        return events, total_bytes

    @classmethod
    def _convert_entity(cls, entity):
        record = dict()
        keys = list(entity.keys()) + list(entity.metadata.keys())
        for key in keys:
            value = entity.get(key, entity.metadata.get(key))
            if isinstance(value, datetime.datetime):
                value = value.isoformat("T")
            elif isinstance(value, UUID):
                value = str(value)
            elif isinstance(value, EntityProperty):
                value = value.value
            # Renaming "timestamp" to "Timestamp" (Compatibility for CosmosDB SDK -> Storage Table SDK)
            if key == "timestamp":
                key = cls._timestamp
            record[key] = value
        return record

    def _process_vm_metrics_table(self, entity):
        if not self._sourcetype == "mscs:vm:metrics":
            return entity
        entity[self._partition_key] = mscs_util.decode_ascii_str(
            entity.get(self._partition_key)
        )
        return entity
