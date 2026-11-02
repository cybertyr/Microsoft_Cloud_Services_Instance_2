#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#

import mscs_logger

from splunklib import modularinput as smi
from mscs_common_utils import set_logger
from modular_inputs.mscs_azure_reservation_recommendation_data_collector import (
    AzureReservationRecommendationDataCollector,
)
from modular_inputs.mscs_azure_usagedetails_data_collector import (
    AzureUsageDetailsDataCollector,
)


class MscsAzureConsumption(smi.Script):
    def __init__(self):
        super(MscsAzureConsumption, self).__init__()

    def get_scheme(self):
        scheme = smi.Scheme("mscs_i2_azure_consumption")
        scheme.description = "Azure Consumption(Billing)"
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
            ("data_type", True),
            ("query_days", False),
            ("start_date", False),
        ]

        for name, is_required in basic_arguments:
            scheme.add_argument(smi.Argument(name, required_on_create=is_required))

        return scheme

    def validate_input(self, definition):
        return

    def _validate_input(self, input_items):
        account_name = input_items.get("account")
        subscription_id = input_items.get("subscription_id")
        data_type = input_items.get("data_type")
        if None in (account_name, subscription_id, data_type):
            raise TypeError(
                "'Azure App Account','Subscription ID' and 'Data Type' are required"
            )

        if data_type not in ["Usage Details", "Reservation Recommendation"]:
            raise ValueError("Invalid value for 'Data Type' field")

        if data_type == "Usage Details":
            try:
                int(input_items.get("query_days", 10))
            except ValueError:
                raise ValueError(
                    "'Max days to query' must be a non-zero positive integer."
                )

    def stream_events(self, inputs, ew):
        self.session_key = inputs.metadata["session_key"]
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
        data_type = input_items.get("data_type")
        try:
            _logger.info("Modular input started")
            self._validate_input(input_items)
            if data_type == "Reservation Recommendation":
                _logger.info("Started Collecting Reservation Recommendation Data")

                reservation_recommendation_collector = (
                    AzureReservationRecommendationDataCollector(
                        _logger, self.session_key, account_name
                    )
                )
                reservation_recommendation_collector.index_reservation_recommendation_data(
                    input_items
                )

                _logger.info("Finished Collecting Reservation Recommendation Data")
            elif data_type == "Usage Details":
                _logger.info("Started Collecting Usage Details Data")

                usage_details_collector = AzureUsageDetailsDataCollector(
                    _logger, self.session_key, account_name
                )
                usage_details_collector.index_usage_details_data(input_items)

                _logger.info("Finished Collecting Usage Details Data")
        except Exception as e:
            _logger.error(
                f'message="An error occurred while collecting data: {e}"',
                exc_info=e,
            )
        finally:
            _logger.info("Modular input exited")
