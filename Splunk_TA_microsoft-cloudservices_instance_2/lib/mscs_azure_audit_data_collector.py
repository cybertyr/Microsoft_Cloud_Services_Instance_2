#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
import json
import time
import urllib.error
import urllib.parse
import urllib.request

import splunktaucclib.data_collection.ta_data_client as dc

import mscs_azure_base_data_collector as mabdc
import mscs_consts
from solnlib.observability import StanzaObservabilityRecorder
import mscs_util


@dc.client_adapter
def do_job_one_time(all_conf_contents, task_config, ckpt):
    data_collector = AzureAuditDataCollector(all_conf_contents, task_config, ckpt)
    return data_collector.collect_data()


class AzureAuditDataCollector(mabdc.AzureBaseDataCollector):

    EVENT_TIMESTAMP = "eventTimestamp"

    def __init__(self, all_conf_contents, task_config, ckpt):
        super(AzureAuditDataCollector, self).__init__(all_conf_contents, task_config)
        self._ckpt = ckpt if ckpt else {}
        self._start_time: str = None
        self._index = None
        self._query_end_time_offset = None
        self._init_from_task_config()
        self._parse_api_setting(mscs_consts.AUDIT)
        self._obs = StanzaObservabilityRecorder(
            "audit", self._logger, self._task_config[mscs_consts.STANZA_NAME]
        )

    def collect_data(self):
        self._logger.info("Starting to collect data.")

        try:
            self._logger.info("Starting to pre-process checkpoint.")
            self._pre_process_ckpt()
            self._logger.info("Finishing pre-process checkpoint.")

            if not self._are_dates_in_correct_order(
                self._ckpt.get(mscs_consts.QUERY_START_TIME),
                self._ckpt.get(mscs_consts.QUERY_END_TIME),
            ):
                self._logger.warning(
                    f'message="start_datetime={self._ckpt.get(mscs_consts.QUERY_START_TIME)} is greater or equal than '
                    f'end_datetime={self._ckpt.get(mscs_consts.QUERY_END_TIME)}. Skipping."',
                )
                return

            stop = yield None, self._ckpt
            if stop:
                self._logger.info("Received the stop signal.")
                return

            while True:
                ckpt_status = self._ckpt[mscs_consts.STATUS]
                if ckpt_status == mscs_consts.CheckpointStatusType.ALL_DONE:
                    break
                url = self._generate_url()

                self._logger.debug("The url=%s", url)

                result = self._perform_request(url)
                result_lst = result.get("value")

                management_events = []

                for index in range(len(result_lst)):
                    management_event = result_lst[index]
                    if len(management_events) >= self._event_cnt_per_item:
                        events, event_bytes = self._convert_management_events_to_events(
                            management_events
                        )
                        self._obs.record(len(management_events), event_bytes)
                        self._ckpt[mscs_consts.CUR_INDEX] = index - 1
                        self._ckpt[
                            mscs_consts.STATUS
                        ] = mscs_consts.CheckpointStatusType.CUR_PAGE_ONGOING
                        stop = yield events, self._ckpt
                        if stop:
                            self._logger.info("Received the stop signal.")
                            return
                        management_events = []

                    if self._is_meet_requirement(index, self._ckpt):
                        management_events.append(management_event)

                next_link = result.get(self._NEXT_LINK)

                self._ckpt[mscs_consts.PAGE_LINK] = next_link
                if len(management_events) > 0:
                    self._ckpt[mscs_consts.CUR_INDEX] = -1

                if next_link:
                    self._ckpt[
                        mscs_consts.STATUS
                    ] = mscs_consts.CheckpointStatusType.CUR_PAGE_DONE
                else:
                    self._ckpt[
                        mscs_consts.STATUS
                    ] = mscs_consts.CheckpointStatusType.ALL_DONE

                events, event_bytes = self._convert_management_events_to_events(
                    management_events
                )
                self._obs.record(len(management_events), event_bytes)
                stop = yield events, self._ckpt
                if stop:
                    self._logger.info("Received the stop signal.")
                    return

            self._logger.info("Finishing collect data.")
        finally:
            self._obs.flush()

    def _generate_url(self):
        page_link = self._ckpt.get(mscs_consts.PAGE_LINK)
        if page_link:
            return page_link
        self._url = self._url.format(
            base_host=self._manager_url,
            subscription_id=self._subscription_id,
            api_version=self._api_version,
        )
        filter_str = self._generate_filter()
        return self._url + "&" + filter_str

    def _generate_filter(self):
        return "$filter=" + urllib.parse.quote(
            "eventTimestamp ge '{start_time}' and "
            "eventTimestamp le '{end_time}' and "
            "eventChannels eq 'Admin, Operation'".format(
                start_time=self._ckpt.get(mscs_consts.QUERY_START_TIME),
                end_time=self._ckpt.get(mscs_consts.QUERY_END_TIME),
            )
        )

    def _init_from_task_config(self):
        self._start_time = self._task_config[mscs_consts.START_TIME]
        self._index = self._task_config[mscs_consts.INDEX]
        tuning_settings = self._global_settings[mscs_consts.PERFORMANCE_TUNING_SETTINGS]

        self._query_end_time_offset = int(
            mscs_util.find_config_in_settings(
                mscs_consts.QUERY_END_TIME_OFFSET,
                mscs_consts.QUERY_END_TIME_OFFSET_DEFAULT_VALUE,
                self._task_config,
                tuning_settings,
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

    def _get_logger_prefix(self):
        account_stanza_name = self._task_config[mscs_consts.ACCOUNT]
        pairs = [
            '{}="{}"'.format(
                mscs_consts.STANZA_NAME, self._task_config[mscs_consts.STANZA_NAME]
            ),
            '{}="{}"'.format(mscs_consts.ACCOUNT, account_stanza_name),
        ]
        return "[{}]".format(" ".join(pairs))

    def _are_dates_in_correct_order(self, query_start_time, query_end_time):
        if query_start_time is None or query_end_time is None:
            return False

        start_time_timestamp = mscs_util.utc_timestr_to_timestamp(query_start_time)
        end_time_timestamp = mscs_util.utc_timestr_to_timestamp(query_end_time)
        return start_time_timestamp < end_time_timestamp

    def _pre_process_ckpt(self):
        ckpt_start_time = self._ckpt.get(mscs_consts.START_TIME)
        query_end_time = mscs_util.timestamp_to_utc(
            time.time() - self._query_end_time_offset
        )

        # When this is the first time to collect data or
        # user modified the start time in conf
        if self._start_time != ckpt_start_time:
            self._logger.info(
                "The start time in conf: %s is not equal to the start time in "
                "checkpoint: %s, reinitialize the checkpoint.",
                self._start_time,
                ckpt_start_time,
            )
            self._init_ckpt(query_end_time)
        else:
            ckpt_status = self._ckpt[mscs_consts.STATUS]
            self._logger.info("The checkpoint status=%s", ckpt_status)

            if ckpt_status == mscs_consts.CheckpointStatusType.ALL_DONE:
                self._logger.info(
                    "The audit logs between %s and %s are all collected, "
                    "updating the checkpoint.",
                    self._ckpt[mscs_consts.QUERY_START_TIME],
                    self._ckpt[mscs_consts.QUERY_END_TIME],
                )

                self._process_all_done_ckpt(query_end_time)
            elif ckpt_status == mscs_consts.CheckpointStatusType.CUR_PAGE_DONE:
                self._logger.info(
                    "Continue to collect entities after page=%s",
                    self._ckpt[mscs_consts.PAGE_LINK],
                )

            elif ckpt_status == mscs_consts.CheckpointStatusType.CUR_PAGE_ONGOING:
                self._logger.info(
                    "Continue to collect entities for page=%s",
                    self._ckpt[mscs_consts.PAGE_LINK],
                )
            else:
                raise Exception(
                    "The checkpoint status={} is invalid.".format(ckpt_status)
                )

    def _init_ckpt(self, query_end_time):
        if not self._are_dates_in_correct_order(self._start_time, query_end_time):
            self._logger.warning(
                f'message="Requested start_datetime={self._start_time} is greater or equal than '
                f'end_datetime={query_end_time}. Skipping checkpoint init."',
            )
            return

        self._ckpt[mscs_consts.START_TIME] = self._start_time
        self._ckpt[mscs_consts.QUERY_START_TIME] = self._start_time
        self._ckpt[mscs_consts.QUERY_END_TIME] = query_end_time
        self._ckpt[mscs_consts.PAGE_LINK] = None
        self._ckpt[mscs_consts.CUR_INDEX] = -1
        self._ckpt[mscs_consts.STATUS] = mscs_consts.CheckpointStatusType.CUR_PAGE_DONE

    def _process_all_done_ckpt(self, query_end_time):
        if not self._are_dates_in_correct_order(
            self._ckpt[mscs_consts.QUERY_END_TIME], query_end_time
        ):
            self._logger.warning(
                f'message="Current end datetime from checkpoint: {self._ckpt[mscs_consts.QUERY_END_TIME]} is greater '
                f'or equal than requested end datetime: {query_end_time}. Skipping checkpoint update."',
            )
            return

        self._ckpt[mscs_consts.QUERY_START_TIME] = self._ckpt[
            mscs_consts.QUERY_END_TIME
        ]
        self._ckpt[mscs_consts.QUERY_END_TIME] = query_end_time
        self._ckpt[mscs_consts.PAGE_LINK] = None
        self._ckpt[mscs_consts.CUR_INDEX] = -1
        self._ckpt[mscs_consts.STATUS] = mscs_consts.CheckpointStatusType.CUR_PAGE_DONE

    def _convert_management_events_to_events(self, management_events):
        events = []
        total_bytes = 0
        for management_event in management_events:
            raw = json.dumps(management_event)
            total_bytes += len(raw.encode())
            events.append(
                dc.build_event(
                    source=management_event["id"],
                    sourcetype=self._sourcetype,
                    index=self._index,
                    raw_data=raw,
                )
            )
        return events, total_bytes

    @classmethod
    def _is_meet_requirement(cls, cur_index, ckpt):
        return cur_index > ckpt[mscs_consts.CUR_INDEX]
