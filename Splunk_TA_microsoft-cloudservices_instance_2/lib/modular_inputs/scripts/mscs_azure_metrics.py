#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
import mscs_logger
from mscs_common_utils import set_logger
from splunklib import modularinput as smi
from modular_inputs.mscs_azure_metrics_resources import AzureMetricsResourceCollector
from modular_inputs.mscs_azure_metrics_collector import AzureMetricsDataCollector
from modular_inputs.mscs_azure_metrics_definitions import AzureMetricsDefinitions


class MscsAzureMetrics(smi.Script):
    def __init__(self):
        super(MscsAzureMetrics, self).__init__()

    def get_scheme(self):
        scheme = smi.Scheme("mscs_i2_azure_metrics")
        scheme.description = "Azure Metrics"
        scheme.use_external_validation = True
        scheme.streaming_mode_xml = True
        scheme.use_single_instance = False

        scheme.add_argument(
            smi.Argument(
                "name", title="Name", description="Name", required_on_create=True
            )
        )

        basic_arguments = [
            ("account", True),
            ("subscription_id", True),
            ("namespaces", True),
            ("metric_statistics", True),
            ("preferred_time_aggregation", True),
            ("metric_index_flag", False),
            ("number_of_threads", True),
        ]

        for name, is_required in basic_arguments:
            scheme.add_argument(smi.Argument(name, required_on_create=is_required))

        return scheme

    def validate_input(self, definition):
        return

    def stream_events(self, inputs, ew):

        self.session_key = self._input_definition.metadata["session_key"]

        input_name = list(inputs.inputs.keys())[0]
        input_items = inputs.inputs[input_name]
        input_items["input_name"] = input_name
        log_file_name = "_".join(
            ["splunk_ta_microsoft-cloudservices_instance_2", input_name.replace("://", "_")]
        )
        _logger = set_logger(self.session_key, log_file_name)
        account_name = input_items.get("account")
        _logger = mscs_logger.PrefixedLoggerAdapter(
            _logger,
            stanza_name=input_name.split("://", 1)[-1],
            account_name=account_name,
        )
        _logger.info("Starting Modular input")

        try:
            subscription_id = input_items.get("subscription_id", "")
            subscription_ids = subscription_id.split(",")
            prf_time_agg = input_items.get("preferred_time_aggregation")
            mtx_stats = input_items.get("metric_statistics")
            no_of_threads = min(int(input_items.get("number_of_threads")), 256)
            namespaces = input_items.get("namespaces", "")
            namespaces_list = namespaces.split(",")
            mtx_namespaces = ",".join(
                f"'{namespace.lower().strip()}'" for namespace in namespaces_list
            )
            query = f"where type in ({mtx_namespaces}) | project id, type"

            collector = AzureMetricsResourceCollector(
                _logger, self.session_key, account_name
            )
            resources = collector.get_resources_by_query(query, subscription_ids)
            _logger.info(f"Count of resources to query: {len(resources)}")

            definitions_collector = AzureMetricsDefinitions(
                _logger, self.session_key, account_name
            )
            definitions_collector.get_metric_definitions_for_resource(resources)

            data_collector = AzureMetricsDataCollector(
                _logger, self.session_key, account_name, definitions_collector
            )
            data_collector.index_metrics_for_resources(
                ew, input_items, prf_time_agg, mtx_stats, resources, no_of_threads
            )

        except Exception as e:
            _logger.error(f'message="Error occurred: {e}"', exc_info=e)

        finally:
            _logger.info("Exiting Modular input")
