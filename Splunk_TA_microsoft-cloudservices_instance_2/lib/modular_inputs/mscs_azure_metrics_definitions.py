#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
import datetime
import hashlib
import traceback

from typing import Dict, List

import mscs_base_data_collector as mbdc
import mscs_common_api_error as mcae
from solnlib.modular_input import checkpointer
from mscs_util import md5

DATE_FORMAT = r"%Y-%m-%dT%H:%M:%SZ"


class AzureMetricsDefinitions(mbdc.AzureBaseDataCollector):
    """
    class responsible for fetching metric definitions
    Args:
        mbdc: AzureBaseDataCollector class is responsible for making API requests
    """

    def __init__(self, logger, session_key, account_name):
        """
        Init method
        :param logger: logger object for logging in file
        :param session_key: session key
        :param account_name: configured account
        """
        super(AzureMetricsDefinitions, self).__init__(logger, session_key, account_name)
        self._parse_api_setting("metrics_defintions")
        self._ckpt = checkpointer.KVStoreCheckpointer(
            "splunk_ta_mscs_i2_metric",
            self._session_key,
            "Splunk_TA_microsoft-cloudservices_instance_2",
        )

    def _generate_url(self, resource_id):
        """
        generate url
        :return url to get metric definitions
        """
        url = self._url.format(
            api_version=self._api_version,
            base_host=self._manager_url.strip("/"),
            resource_id=resource_id.strip("/"),
        )
        return url

    def _get_metrics_definitions_by_ckpt_type_key(
        self, resources_to_query: List[Dict]
    ) -> Dict[str, Dict]:
        metrics_definitions_by_type = {}
        unique_res_types = {resource_obj["type"] for resource_obj in resources_to_query}
        for res_type in unique_res_types:
            ckpt_key = res_type.replace("/", "_")
            metric_definitions = self._ckpt.get(ckpt_key)
            metrics_definitions_by_type[ckpt_key] = metric_definitions

        return metrics_definitions_by_type

    def _is_outdated(self, last_updated):
        utc_now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        return last_updated < utc_now - datetime.timedelta(days=30)

    def _refresh_outdated_metric_definitions(
        self, resource_obj: Dict, metric_definitions: Dict, ckpt_key: str
    ):

        self._logger.debug(
            "Found metric definitions for namespace {} in the checkpoint.".format(
                resource_obj["type"]
            )
        )
        # Update the metric checkpoint if the definitions were fetched more than 30 days ago
        metrics_last_updated = datetime.datetime.strptime(
            metric_definitions["last_updated_date"], DATE_FORMAT
        )
        if self._is_outdated(metrics_last_updated):
            self._logger.debug(
                "Metric definitions may be out of date.  The last update for namespace '{}' was on '{}'. Polling Azure API for the definitions.".format(
                    resource_obj["type"], str(metrics_last_updated)
                )
            )
            self.get_metric_definitions(
                resource_obj["id"], ckpt_key, resource_obj["type"]
            )

    def get_metric_definitions_for_resource(self, resources_to_query):
        """
        getting metric definitions
        :param resources_to_query: list of resources
        """
        metrics_def_by_ckpt_type_key = self._get_metrics_definitions_by_ckpt_type_key(
            resources_to_query
        )

        for resource_obj in resources_to_query:
            hash_res_id = md5(resource_obj["id"])

            ckpt_key = (
                f"{resource_obj['type'].replace('/', '_')}_{hash_res_id.hexdigest()}"
            )

            # Attempt to fetch metric definitions unique for resource object
            metric_definitions = self._ckpt.get(ckpt_key)

            # If not found, fallback to using the type-based metric definitions
            if metric_definitions is None:
                ckpt_key = resource_obj["type"].replace("/", "_")
                metric_definitions = metrics_def_by_ckpt_type_key.get(
                    resource_obj["type"]
                )

            if metric_definitions is not None:
                self._refresh_outdated_metric_definitions(
                    resource_obj, metric_definitions, ckpt_key
                )

            else:
                # Metric definitions were not found in the checkpoint, so go query the Azure API endpoint for the
                # metric definitions and store them in the checkpoint.
                self._logger.debug(
                    "Metric defininitions for namespace {} were not found in the checkpoint.  Polling Azure API for the metric definitions.".format(
                        ckpt_key
                    )
                )
                metric_definitions = self.get_metric_definitions(
                    resource_obj["id"], ckpt_key, resource_obj["type"]
                )
                if metric_definitions is None:
                    self._logger.warning(
                        f'message="No metric definitions found for namespace {ckpt_key}."'
                    )

    def get_metric_definitions(self, resource_id, ckpt_key, resource_type):
        """
        get metric definitions
        :param resource_id: id of the resource
        :param ckpt_key: checkpoint key (key format: <resource_type> or <resource_type>_<hashed_resource_id>)
        :param resource_type: resource type
        """
        metric_definitions = []
        metrics = []

        try:
            url = self._generate_url(resource_id)
            response = self._perform_request(url, "get")
            metric_definitions.extend(response["value"])

            for metric in metric_definitions:
                metric_obj = {}
                metric_obj["name"] = metric["name"]["value"]

                time_grains = []
                retention_by_time_grain = {}
                for time_grain in metric.get("metricAvailabilities", []):
                    time_grain_value = time_grain["timeGrain"]
                    time_grains.append(time_grain_value)
                    retention = time_grain.get("retention")
                    if retention:
                        retention_by_time_grain[time_grain_value] = retention
                metric_obj["time_grains"] = time_grains
                if retention_by_time_grain:
                    metric_obj["retention_by_time_grain"] = retention_by_time_grain

                aggregation_types = []
                for aggregation_type in metric.get("supportedAggregationTypes", []):
                    aggregation_types.append(aggregation_type.lower())
                metric_obj["aggregation_types"] = aggregation_types

                metrics.append(metric_obj)

            if len(metrics) > 0:
                self._save_metric_definitions(metrics, ckpt_key, resource_type)

            return metrics
        except mcae.APIError as e:
            self._logger.error(
                f'message="Error occurred while fetching metric definition - status_code: {str(e.status)}, error: {str(e.result)}"',
                exc_info=e,
            )
        except Exception as e:
            self._logger.error(
                f'message="Error occurred while fetching metric definition: {e}"',
                exc_info=e,
            )

    def _save_metric_definitions(self, metrics, ckpt_key, resource_type):
        """
        checkpoint metrics definitions
        :param metrics: list of metrics definitions
        :param ckpt_key: ckpt_key (key format: <resource_type> or <resource_type>_<hashed_resource_id>)
        :param resource_type: resource type
        """
        retention_metadata_available = any(
            metric.get("retention_by_time_grain") for metric in metrics
        )
        # checkpoint the metric definition
        checkpoint_data = {
            "last_updated_date": datetime.datetime.now(datetime.timezone.utc).strftime(
                DATE_FORMAT
            ),
            "resource_type": resource_type,
            "retention_metadata_available": retention_metadata_available,
            "metrics": metrics,
        }
        self._logger.debug(
            "Saving metric definitions from Azure API for '{}'.".format(ckpt_key)
        )
        self._ckpt.update(ckpt_key, checkpoint_data)
        return checkpoint_data
