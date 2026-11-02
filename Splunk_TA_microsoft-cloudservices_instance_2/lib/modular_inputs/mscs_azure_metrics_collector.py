#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
import logging
import sys
import json
import datetime
import re
import traceback
import math
import hashlib
import os
import signal
import threading
from typing import Dict, List

import isodate
import mscs_base_data_collector as mbdc
import mscs_common_api_error as mcae
from concurrent import futures
from solnlib.modular_input import checkpointer
from splunklib import modularinput as smi

from modular_inputs.mscs_azure_metrics_definitions import AzureMetricsDefinitions
from solnlib.observability import StanzaObservabilityRecorder
from mscs_util import md5

ONE_DAY_IN_SECONDS = 86400
DATE_FORMAT = r"%Y-%m-%dT%H:%M:%S%z"  # We save in timezone aware, but convert to UTC before request
DATE_FORMAT_CKPT = r"%Y-%m-%dT%H:%M:%SZ"  # Downgrade compatibility
MAX_METRIC_LIST = 20
API_DELAY = datetime.timedelta(seconds=120)
# Compatibility fallback when retention metadata is unavailable.
# Azure Monitor platform/custom metrics are retained for 93 days, except documented cases:
# https://learn.microsoft.com/en-us/azure/azure-monitor/metrics/data-platform-metrics#retention-of-metrics
DEFAULT_METRICS_RETENTION = datetime.timedelta(days=93)

stop_collection = threading.Event()


def exit_gracefully(signum, frame):
    global stop_collection
    stop_collection.set()


class SystemExitError(Exception):
    pass


class MismatchMetricsDefinitionError(mcae.APIError):
    pass


class AzureMetricsDataCollector(mbdc.AzureBaseDataCollector):
    """
    class responsible for collecting metric data for resources
    Args:
        mbdc: AzureBaseDataCollector class is responsible for making API requests
    """

    def __init__(
        self,
        logger: logging.Logger,
        session_key: str,
        account_name: str,
        definitions_collector: AzureMetricsDefinitions,
    ):
        """
        Init method
        :param logger: logger object for logging in file
        :param session_key: session key
        :param account_name: configured account
        """
        super(AzureMetricsDataCollector, self).__init__(
            logger, session_key, account_name
        )
        self._definitions_collector = definitions_collector
        self._parse_api_setting("metrics")
        self._ckpt = checkpointer.KVStoreCheckpointer(
            "splunk_ta_mscs_i2_metric",
            self._session_key,
            "Splunk_TA_microsoft-cloudservices_instance_2",
        )
        self._obs = None

    def _date_to_str(self, date_obj):
        """
        Convert date to str
        :param date_obj: date in datetime
        :return date in str
        """
        return date_obj.strftime(DATE_FORMAT)

    def _date_to_str_without_timezone(self, date_obj):
        """
        Convert date to str without timezone
        :param date_obj: date in datetime
        :return date in str
        """
        return date_obj.strftime(DATE_FORMAT_CKPT)

    def _str_to_date(self, str_obj):
        """
        Convert str to date
        :param str_obj: date in str
        :return date in datetime
        """
        return datetime.datetime.strptime(str_obj, DATE_FORMAT)

    def _str_to_date_without_timezone(self, str_obj):
        """
        Convert str without timezone to date
        :param str_obj: date in str
        :return date in datetime
        """
        return datetime.datetime.strptime(str_obj, DATE_FORMAT_CKPT)

    def _split(self, start, end, gap):
        """
        Split given range keeping given gap in between.

        E.g.
        >> list(split(1, 13, 4))
        >> [(1, 5), (5, 10), (10, 13)]
        """
        assert start <= end
        start_ = start

        while True:
            end_ = min(start_ + gap, end)
            yield (start_, end_)
            start_ = end_
            if end_ >= end:
                break

    def _divide(self, start, end, divisor):
        """
        Divide the given start & end range.

        E.g.
        >> list(divide(1, 10, 2))
        >> [[1, 5], [6, 10]]
        """
        if divisor < 2:
            return [
                (start, end),
            ]
        gap = int(math.ceil((end - start) / divisor))
        return self._split(start, end, gap)

    def _get_metric_timespan(self, ckpt_key, retention_window=None):
        """Build the next metrics query window from checkpoint state."""
        end_time = datetime.datetime.now(tz=datetime.timezone.utc) - API_DELAY
        start_time = None
        ckpt_data = self._ckpt.get(ckpt_key)
        if ckpt_data:
            start_time_ckpt = ckpt_data.get("end_time")
            if start_time_ckpt:
                start_time = self._str_to_date_without_timezone(
                    start_time_ckpt
                ).replace(tzinfo=datetime.timezone.utc)

        if start_time is None:
            # Preserve the original first-run two-minute query window.
            start_time = end_time - API_DELAY
        else:
            min_start_time = end_time - (retention_window or DEFAULT_METRICS_RETENTION)
            max_start_time = end_time - datetime.timedelta(seconds=60)
            if start_time < min_start_time:
                self._logger.warning(
                    "Checkpoint end_time %s is older than the Azure metrics "
                    "retention window. Clamping the query window to %s/%s.",
                    self._date_to_str_without_timezone(start_time),
                    self._date_to_str_without_timezone(min_start_time),
                    self._date_to_str_without_timezone(end_time),
                )
                start_time = min_start_time
            elif start_time > max_start_time:
                self._logger.warning(
                    "Checkpoint end_time %s is ahead of the safe metrics window. "
                    "Clamping the query window to %s/%s.",
                    self._date_to_str_without_timezone(start_time),
                    self._date_to_str_without_timezone(max_start_time),
                    self._date_to_str_without_timezone(end_time),
                )
                start_time = max_start_time

        return start_time, end_time

    def _ensure_metric_retention_metadata(
        self,
        resource_obj: Dict,
        metric_definitions_ckpt: Dict,
        mtx_def_by_id_ckpt_key: str,
    ) -> List[Dict]:
        """Refresh legacy checkpoints that predate retention metadata."""
        metric_definitions = metric_definitions_ckpt["metrics"]

        if "retention_metadata_available" in metric_definitions_ckpt:
            return metric_definitions

        self._logger.info(
            "Metric definitions for resource %s were saved before retention "
            "metadata was checkpointed. Refreshing definitions from Azure.",
            resource_obj["id"],
        )
        refreshed_metric_definitions = (
            self._definitions_collector.get_metric_definitions(
                resource_obj["id"], mtx_def_by_id_ckpt_key, resource_obj["type"]
            )
        )
        return refreshed_metric_definitions or metric_definitions

    def _get_metric_retention_window(
        self, metric_definition: dict, preferred_time_aggregation: str
    ):
        retention_by_time_grain = metric_definition.get("retention_by_time_grain")
        if not retention_by_time_grain:
            return None

        if preferred_time_aggregation in retention_by_time_grain:
            candidate_durations = [retention_by_time_grain[preferred_time_aggregation]]
        else:
            candidate_durations = retention_by_time_grain.values()

        retention_windows = []
        for duration in candidate_durations:
            if not duration:
                continue
            try:
                retention_window = isodate.parse_duration(duration)
            except (TypeError, ValueError, OverflowError, AttributeError):
                continue
            if isinstance(retention_window, datetime.timedelta):
                retention_windows.append(retention_window)
        if not retention_windows:
            return None

        return min(retention_windows)

    def _resolve_retention_window(
        self, metric_definitions: List[Dict], preferred_time_aggregation: str
    ):
        retention_windows = []
        for metric_definition in metric_definitions:
            retention_window = self._get_metric_retention_window(
                metric_definition, preferred_time_aggregation
            )
            if retention_window is not None:
                retention_windows.append(retention_window)

        if retention_windows:
            return min(retention_windows)

        self._logger.debug(
            "Metric definitions do not include retention data. Falling back to "
            "the default Azure metrics retention window of %s.",
            DEFAULT_METRICS_RETENTION,
        )
        return DEFAULT_METRICS_RETENTION

    def _generate_metrics_url(self, resource_id):
        """
        generate metrics url
        :return url to get metrics data
        """
        url = self._url.format(
            api_version=self._api_version,
            base_host=self._manager_url.strip("/"),
            resource_id=resource_id.strip("/"),
        )
        return url

    def _chunk_metric_list(self, metrics, chunk_size):
        """
        chunk metric list
        :param metrics: list of metric names
        :param chunk_size: chunk size
        """
        for i in range(0, len(metrics), chunk_size):
            yield metrics[i : i + chunk_size]

    def _timespan_formatter(self, chunked_time_list):
        # This has to be UTC as that is what Azure Metrics API wants
        # Local timestamp (why is it a timestamp) -> UTC
        convert_date_to_utc_str = lambda timestamp: datetime.datetime.strftime(
            datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc),
            DATE_FORMAT_CKPT,
        )

        for tmp_start_time_, tmp_end_time_ in chunked_time_list:
            start_time = convert_date_to_utc_str(tmp_start_time_)
            end_time = convert_date_to_utc_str(tmp_end_time_)
            yield start_time, end_time

    def _is_metric_configuration_error(self, error: mcae.APIError):
        return (
            error.status == 400
            and "Failed to find metric configuration for provider"
            in error.result.get("message")
        )

    def _index_metrics(
        self,
        ew,
        input_items,
        mtx_ckpt_key,
        end_time,
        resource_obj,
        metric_url,
        req_params,
        requested_mtx_stats,
        metric_aggregations,
    ):
        """
        get metrics and index data
        :param ew: event writer to write event
        :param input_items: input details
        :param mtx_ckpt_key: metrics checkpoint key
        :param end_time: end time
        :param resource_obj: resource with id and type
        :param metric_url: metric url
        :param req_params: request params
        :param requested_mtx_stats: requested metrics statistics
        :param metric_aggregations: metric aggregations
        """

        # Extract the subscription ID to include in the event
        subscription_id = re_res_grp = ""
        requested_metrics_statistics = requested_mtx_stats.split(",")
        re_sub = re.compile(r"subscriptions\/(.*?)\/")
        re_res_grp = re.compile(r"resourceGroups\/(.*?)\/")
        try:
            subscription_id = re_sub.search(resource_obj["id"].lower()).group(1)
            resource_group = re_res_grp.search(resource_obj["id"]).group(1)
        except Exception as e:
            self._logger.error(
                'message="subscription_id or resource_group parsing failed with error"',
                exc_info=e,
            )
            raise

        resource_metrics = self._fetch_resource_metrics(
            metric_url, req_params, resource_obj
        )

        endtime = None
        metric_name = None
        ckpt_data = {}
        try:
            for metric in resource_metrics:
                endtime = None
                event_count_ingested = 0
                event_bytes_ingested = 0
                event_count_skipped = 0
                metric_name = metric["name"]["value"]
                metric_unit = metric["unit"]
                ckpt_mtx_time = None
                ckpt_data = self._ckpt.get(mtx_ckpt_key)
                if ckpt_data:
                    ckpt_mtx_time = ckpt_data.get(metric_name)
                else:
                    ckpt_data = {}

                for timeSeries in metric.get("timeseries", []):
                    for data in timeSeries.get("data", []):
                        endtime = None
                        if (ckpt_mtx_time is None) or (
                            self._str_to_date_without_timezone(data["timeStamp"])
                            >= self._str_to_date_without_timezone(
                                ckpt_mtx_time
                            ).replace(second=0)
                        ):
                            metric_obj = {}
                            metric_obj["resource_id"] = resource_obj["id"]
                            metric_obj["metric_name"] = metric_name
                            metric_obj["timeStamp"] = data["timeStamp"]
                            metric_obj["subscription_id"] = subscription_id
                            metric_obj["unit"] = metric_unit
                            metric_obj["namespace"] = resource_obj["type"]
                            metric_obj["resource_group"] = resource_group
                            # https://learn.microsoft.com/en-us/azure/azure-monitor/essentials/rest-api-walkthrough?tabs=portal
                            # 2023-09-19T02:00:00Z
                            endtime = data["timeStamp"]

                            is_metric_stat = False
                            for metric_stat in requested_metrics_statistics:
                                # Only collect the requested statistics
                                try:
                                    if metric_stat in metric_aggregations[metric_name]:
                                        if metric_stat in data:
                                            is_metric_stat = True
                                            metric_obj[metric_stat] = data[metric_stat]
                                except KeyError as e:
                                    if metric_stat in data:
                                        is_metric_stat = True
                                        metric_obj[metric_stat] = data[metric_stat]
                                    self._logger.error(
                                        f'message="Error {e} occurred"',
                                        exc_info=e,
                                    )
                            try:
                                if not is_metric_stat:
                                    event_count_skipped += 1
                                    continue
                                raw = json.dumps(metric_obj)
                                event = smi.Event(
                                    data=raw,
                                    index=input_items.get("index"),
                                    sourcetype=input_items.get("sourcetype"),
                                    time="%.3f"
                                    % (
                                        self._str_to_date(data["timeStamp"]).timestamp()
                                    ),
                                )
                                ew.write_event(event)
                                event_count_ingested += 1
                                event_bytes_ingested += len(raw.encode())
                            except BrokenPipeError:
                                self._logger.info("Output stream was closed")
                            finally:
                                # added 60 sec as the start time is inclusive
                                ckpt_end_time = self._str_to_date_without_timezone(
                                    data["timeStamp"]
                                ).replace(
                                    tzinfo=datetime.timezone.utc
                                ) + datetime.timedelta(
                                    seconds=60
                                )
                                ckpt_data[
                                    metric_name
                                ] = self._date_to_str_without_timezone(ckpt_end_time)
                                if stop_collection.is_set():
                                    self._logger.info(
                                        "Saving checkpoint for input before termination due to SIGTERM."
                                    )
                                    self._ckpt.update(mtx_ckpt_key, ckpt_data)
                                    raise SystemExitError

                ckpt_data[metric_name] = end_time
                self._ckpt.update(mtx_ckpt_key, ckpt_data)
                if event_count_skipped:
                    self._logger.debug(
                        "Events Skipped due to no metric statistics found. resource={}, metric_name={}, count={}".format(
                            resource_obj["id"], metric_name, event_count_skipped
                        )
                    )
                if event_count_ingested:
                    self._logger.debug(
                        "Events ingested. resource={}, metric_name={}, count={}".format(
                            resource_obj["id"], metric_name, event_count_ingested
                        )
                    )
                    if self._obs:
                        self._obs.record(event_count_ingested, event_bytes_ingested)
        except SystemExitError as e:
            raise e
        except Exception as e:
            if endtime and metric_name:
                # added 60 sec as the start time is inclusive
                ckpt_end_time = datetime.datetime.strptime(
                    endtime, DATE_FORMAT_CKPT
                ).replace(tzinfo=datetime.timezone.utc) + datetime.timedelta(seconds=60)
                ckpt_data[metric_name] = self._date_to_str_without_timezone(
                    ckpt_end_time
                )
                self._ckpt.update(mtx_ckpt_key, ckpt_data)
            self._logger.error(
                'message="Error occurred while fetching resource metrics"',
                exc_info=e,
            )
            raise

    def _fetch_resource_metrics(
        self, metric_url: str, req_params: Dict, resource_obj: Dict
    ) -> List:
        """
        Fetch resource metrics from the provided URL.
        :param metric_url: metric url
        :param req_params: request params
        :param resource_obj: resource with id and type
        :return list of resource metrics
        """
        try:
            response = self._perform_request(
                metric_url, "get", req_params, log_error=False
            )
            return response.get("value", [])
        except mcae.APIError as e:
            if self._is_metric_configuration_error(e):
                raise MismatchMetricsDefinitionError
            self._logger.error(
                f'message="Error occurred while fetching resource metrics - status_code: {str(e.status)}, error: {str(e.result)}"',
                exc_info=e,
            )
            raise
        except Exception as e:
            resource_id = resource_obj["id"]
            self._logger.error(
                f'message="Error occurred while fetching resource metrics. resource={resource_id}, error={str(e)}"',
                exc_info=e,
            )
            raise

    def _index_resource_metrics(
        self,
        ew: smi.EventWriter,
        input_items: dict,
        resource_obj: dict,
        prf_time_agg: str,
        requested_mtx_stats: str,
        retry: bool = False,
    ):
        """
        index resource metrics
        :param ew: event writer to write event
        :param input_items: input details
        :param resource_obj: resource with id and type
        :param prf_time_agg: preferred time aggregations
        :param requested_mtx_stats: requested metrics statistics
        :param retry: flag to control if it's a retry attempt (default: False)
        """
        is_success = True
        input_name = input_items.get("input_name")
        hash_res_id = md5(resource_obj["id"])

        mtx_ckpt_key_unhashed = "".join(
            [input_name.replace("://", "_"), "_", resource_obj["id"]]
        )
        mtx_ckpt_key = "".join(
            [input_name.replace("://", "_"), "_", hash_res_id.hexdigest()]
        )
        self._logger.debug(
            "Original key={}, hashed key={}".format(mtx_ckpt_key_unhashed, mtx_ckpt_key)
        )
        # Metrics that support the prf_time_agg will go in this list
        metrics_preferred = []

        # All other metrics will go in this list.
        # For example, the 'microsoft.storage/storageaccounts' namespace contains a metric called 'Used capacity' that only supports PT1H (1 hour).
        # Other metrics in this namespace support PT1M (1 minute).
        # If the user requested PT1M, the 'Used capacity' metric will go in the alternate list while other metrics that support PT1M go in the preferred list.
        metrics_alternate = []

        # Keep a dict of supported aggregations (average, minimum, maximum, etc) to compare against requested statistics later.
        metric_aggregations = {}

        # First try to get metric definitions by resource ID
        mtx_def_by_id_ckpt_key = (
            f"{resource_obj['type'].replace('/', '_')}_{hash_res_id.hexdigest()}"
        )
        mtx_def_from_ckpt = self._ckpt.get(mtx_def_by_id_ckpt_key)

        if mtx_def_from_ckpt is None:
            mtx_def_by_type_ckpt_key = resource_obj["type"].replace("/", "_")
            mtx_def_from_ckpt = self._ckpt.get(mtx_def_by_type_ckpt_key)

            if mtx_def_from_ckpt is None:
                self._logger.warning(
                    f'message="No metric definitions found for namespace {mtx_def_by_type_ckpt_key}."'
                )
                return

        metric_definitions = self._ensure_metric_retention_metadata(
            resource_obj, mtx_def_from_ckpt, mtx_def_by_id_ckpt_key
        )
        retention_window = self._resolve_retention_window(
            metric_definitions, prf_time_agg
        )
        starttime, endtime = self._get_metric_timespan(mtx_ckpt_key, retention_window)
        starttime_ = self._date_to_str(starttime)
        endtime_ = self._date_to_str(endtime)
        starttime_utc_timestamp = starttime.timestamp()
        endtime_utc_timestamp = endtime.timestamp()

        # Populate the preferred and alternate lists.
        for metric_definition in metric_definitions:
            if prf_time_agg in metric_definition["time_grains"]:
                metrics_preferred.append(metric_definition["name"])
            else:
                metrics_alternate.append(metric_definition["name"])

            if metric_definition["name"] not in metric_aggregations:
                metric_aggregations[metric_definition["name"]] = metric_definition[
                    "aggregation_types"
                ]

        self._logger.debug("Metric Aggregations: {}".format(metric_aggregations))

        # The Azure REST API will only accept 20 metrics at a time, so we may need to create multiple lists.
        metric_list_preferred = list(
            self._chunk_metric_list(metrics_preferred, MAX_METRIC_LIST)
        )
        self._logger.debug("Preferred metric list: {}".format(metric_list_preferred))
        metric_list_alternate = list(
            self._chunk_metric_list(metrics_alternate, MAX_METRIC_LIST)
        )
        self._logger.debug("Alternate metric list: {}".format(metric_list_alternate))

        divisor = int(
            math.ceil(
                (endtime_utc_timestamp - starttime_utc_timestamp) / ONE_DAY_IN_SECONDS
            )
        )
        chunked_time_list = self._divide(
            starttime_utc_timestamp, endtime_utc_timestamp, divisor=divisor
        )
        metric_url = self._generate_metrics_url(resource_obj["id"])

        self._logger.info(
            "Collecting the data from timespan: {}/{}".format(starttime_, endtime_)
        )

        try:
            for metric_list in metric_list_preferred:
                mtx_name_list = ",".join(metric_list)
                # chunked the timespan in gap of 1 day
                # to avoid error -> response gt 8MB
                st_et_list = self._timespan_formatter(chunked_time_list)
                for start_time, end_time in st_et_list:
                    # The timespan of the query. It is a string with the following format 'startDateTime_ISO/endDateTime_ISO'.
                    # Example: ...&timespan=2017-04-14T02:20:00Z/2017-04-14T04:20:00Z
                    metric_timespan_ = "{}/{}".format(start_time, end_time)
                    self._logger.info(
                        "Chunked metrics timespan: {} for resource {}".format(
                            metric_timespan_, resource_obj["id"]
                        )
                    )
                    req_params = {
                        "timespan": metric_timespan_,
                        "interval": prf_time_agg,
                        "metricnames": mtx_name_list,
                        "aggregation": requested_mtx_stats,
                    }
                    self._index_metrics(
                        ew,
                        input_items,
                        mtx_ckpt_key,
                        end_time,
                        resource_obj,
                        metric_url,
                        req_params,
                        requested_mtx_stats,
                        metric_aggregations,
                    )

            for metric_list in metric_list_alternate:
                mtx_name_list = ",".join(metric_list)
                st_et_list = self._timespan_formatter(chunked_time_list)
                for start_time, end_time in st_et_list:
                    metric_timespan_ = "{}/{}".format(start_time, end_time)
                    self._logger.info(
                        "Chunked metrics timespan: {} for resource {}".format(
                            metric_timespan_, resource_obj["id"]
                        )
                    )
                    req_params = {
                        "timespan": metric_timespan_,
                        "metricnames": mtx_name_list,
                        "aggregation": requested_mtx_stats,
                    }
                    self._index_metrics(
                        ew,
                        input_items,
                        mtx_ckpt_key,
                        end_time,
                        resource_obj,
                        metric_url,
                        req_params,
                        requested_mtx_stats,
                        metric_aggregations,
                    )

        except MismatchMetricsDefinitionError:
            result = self._handle_metric_definition_mismatch(
                ew,
                input_items,
                resource_obj,
                prf_time_agg,
                requested_mtx_stats,
                mtx_def_by_id_ckpt_key,
                retry,
            )
            is_success = result
        except Exception:
            # setting is_success=False due to exception to avoid ckpt update
            is_success = False

        if is_success:
            checkpoint_mtx_data = self._ckpt.get(mtx_ckpt_key) or {}
            checkpoint_end_time = endtime
            safe_end_time = datetime.datetime.now(tz=datetime.timezone.utc) - API_DELAY
            if checkpoint_end_time > safe_end_time:
                self._logger.warning(
                    "Preventing checkpoint end_time %s from moving ahead of the "
                    "safe metrics window. Using %s instead.",
                    self._date_to_str_without_timezone(checkpoint_end_time),
                    self._date_to_str_without_timezone(safe_end_time),
                )
                checkpoint_end_time = safe_end_time
            checkpoint_mtx_data["end_time"] = self._date_to_str_without_timezone(
                checkpoint_end_time
            )
            self._ckpt.update(mtx_ckpt_key, checkpoint_mtx_data)

    def _handle_metric_definition_mismatch(
        self,
        event_writer: smi.EventWriter,
        input_items: dict,
        resource_obj: dict,
        prf_time_agg: str,
        requested_mtx_stats: str,
        mtx_def_by_id_ckpt_key: str,
        retry: bool,
    ) -> bool:
        """Handle the case where a mismatch in metric definitions occurs."""
        resource_id = resource_obj["id"]
        if not retry:
            self._logger.warning(
                f'message="Attempting to retrieve updated metrics for resource {resource_id} and retrying indexing."'
            )

            metrics = self._definitions_collector.get_metric_definitions(
                resource_id, mtx_def_by_id_ckpt_key, resource_obj["type"]
            )

            if not metrics:
                # To avoid continuous retry, save empty metrics definitions
                self._definitions_collector._save_metric_definitions(
                    [], mtx_def_by_id_ckpt_key, resource_obj["type"]
                )
                return False

            self._index_resource_metrics(
                event_writer,
                input_items,
                resource_obj,
                prf_time_agg,
                requested_mtx_stats,
                retry=True,
            )
            return True
        else:
            self._logger.error(
                f'message="MismatchMetricsDefinitionError occurred again after retrying for resource {resource_id}"'
            )
            return False

    def index_metrics_for_resources(
        self,
        ew,
        input_items,
        prf_time_agg,
        mtx_stats,
        resources_to_query,
        no_of_threads=5,
    ):
        """
        index metrics for resource
        :param ew: event writer to write event
        :param input_items: input details
        :param prf_time_agg: preferred time aggregations
        :param mtx_stats: metrics statistics
        :param resources_to_query: resources to query
        :param no_of_threads: number of threads
        """
        stanza_name = input_items.get("input_name", "").split("://", 1)[-1]
        self._obs = StanzaObservabilityRecorder("metrics", self._logger, stanza_name)

        signal.signal(signal.SIGINT, exit_gracefully)
        signal.signal(signal.SIGTERM, exit_gracefully)

        # for windows machine
        if os.name == "nt":
            signal.signal(signal.SIGBREAK, exit_gracefully)

        with self._obs:
            with futures.ThreadPoolExecutor(max_workers=int(no_of_threads)) as executor:
                metrics_future = dict(
                    (
                        executor.submit(
                            self._index_resource_metrics,
                            ew,
                            input_items,
                            resource_obj,
                            prf_time_agg,
                            mtx_stats,
                            False,
                        ),
                        resource_obj,
                    )
                    for resource_obj in resources_to_query
                )

                for future in futures.as_completed(metrics_future, None):
                    resource = metrics_future[future]
                    if future.exception() is not None:
                        if isinstance(future.exception(), SystemExitError):
                            sys.exit(0)
                        else:
                            self._logger.error(
                                f'message="Error occurred while fetching resource metrics for resource={resource}, error={future.exception()}"'
                            )
