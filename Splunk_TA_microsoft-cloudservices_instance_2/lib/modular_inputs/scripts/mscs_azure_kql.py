#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
import sys
import mscs_logger
from modular_inputs.mscs_azure_kql_collector import AzureKQLCollector
from mscs_common_utils import set_logger
from splunklib import modularinput as smi
from solnlib import utils


class MSCSAzureKQL(smi.Script):
    def __init__(self):
        super(MSCSAzureKQL, self).__init__()

    def get_scheme(self):
        """Get Azure KQL input scheme."""
        scheme = smi.Scheme("mscs_i2_azure_kql")
        scheme.description = "Azure KQL Log Analytics"
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
            ("kql_query", True),
            ("workspace_id", True),
            ("index_stats", False),
            ("index_empty_values", False),
        ]

        for name, is_required in basic_arguments:
            scheme.add_argument(smi.Argument(name, required_on_create=is_required))

        return scheme

    def validate_input(self, definition):
        return

    @classmethod
    def sanitize_input(cls, input_stanza):
        """Sanitize the user provided input."""
        input_stanza = {key: val.strip() for key, val in input_stanza.items()}
        input_stanza["index_empty_values"] = utils.is_true(
            input_stanza.get("index_empty_values")
        )
        input_stanza["index_stats"] = utils.is_true(input_stanza.get("index_stats"))
        return input_stanza

    def stream_events(self, inputs, ew):
        """Fetch, collect and stream events to Splunk."""
        session_key = self._input_definition.metadata["session_key"]
        if len(inputs.inputs) != 1:
            sys.exit(0)

        stanza_name, input_stanza = list(inputs.inputs.items())[0]
        input_type, input_name = stanza_name.split("://")
        input_stanza["name"] = input_name
        input_stanza["input_type"] = input_type

        logfile_name = f"splunk_ta_microsoft-cloudservices_instance_2_{input_type}_{input_name}"
        logger = set_logger(session_key, logfile_name)
        account_name = input_stanza.get("account")
        logger = mscs_logger.PrefixedLoggerAdapter(
            logger, stanza_name=input_name, account_name=account_name
        )

        try:
            logger.info("Starting the modular input")
            input_stanza = self.sanitize_input(input_stanza)
            collector = AzureKQLCollector(input_stanza, session_key, logger)
            collector.start()
        except Exception as e:
            logger.error(f'message="Unknown error occurred: {e}"', exc_info=e)
        finally:
            logger.info("Exiting the modular input")
