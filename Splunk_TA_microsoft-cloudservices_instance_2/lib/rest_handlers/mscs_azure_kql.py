#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
import mscs_util
from splunk import admin
import traceback
import json

from splunktaucclib.rest_handler.endpoint import (
    field,
    validator,
    RestModel,
    DataInputModel,
)

from mscs_common_utils import set_logger
from modular_inputs.scripts.mscs_i2_azure_kql import MSCSAzureKQL
from modular_inputs.mscs_azure_kql_collector import AzureKQLCollector


class GetSessionKey(admin.MConfigHandler):
    def __init__(self):
        self.session_key = self.getSessionKey()


class MSCSAzureKQLValidator(validator.Validator):
    def __init__(self):
        super(MSCSAzureKQLValidator, self).__init__()

    def validate(self, value, data):
        session_key = GetSessionKey().session_key
        log_file_name = "splunk_ta_microsoft-cloudservices_instance_2_rh_mscs_azure_kql"
        logger = set_logger(session_key, log_file_name)

        try:
            logger.info("Starting validation of KQL Log Analytics user input")
            data = MSCSAzureKQL.sanitize_input(data)
            collector = AzureKQLCollector(data, session_key, logger)
            collector.validate_kql_query()

        except Exception as ex:
            error_msg = str(ex)
            if getattr(ex, "inner_error_msg", None):
                error_msg = ex.inner_error_msg
                logger.error(
                    f'message="Received error along with events, status code: {ex.status}, error: {json.dumps(ex.error)}"'
                )
            else:
                logger.error(
                    f'message="Error occured while validating KQL Log Analytics user input."',
                    exc_info=ex,
                )

            self.put_msg(f"Error occured while validating KQL Query: {error_msg}")
            return False

        else:
            logger.info("Completed validation of KQL Log Analytics user input")
            return True


fields = [
    field.RestField(
        "account", required=True, encrypted=False, default=None, validator=None
    ),
    field.RestField(
        "workspace_id",
        required=True,
        encrypted=False,
        default=None,
        validator=validator.String(
            max_len=1000,
            min_len=1,
        ),
    ),
    field.RestField(
        "kql_query",
        required=True,
        encrypted=False,
        default=None,
        validator=MSCSAzureKQLValidator(),
    ),
    field.RestField(
        "interval",
        required=True,
        encrypted=False,
        default="3600",
        validator=validator.AllOf(
            validator.Pattern(
                regex=r"""^[1-9]\d*$""",
            ),
            validator.Number(
                max_val=31536000,
                min_val=1,
            ),
        ),
    ),
    field.RestField(
        "index",
        required=True,
        encrypted=False,
        default=None,
        validator=mscs_util.MscsAzureIndexValidator(),
    ),
    field.RestField(
        "sourcetype",
        required=True,
        encrypted=False,
        default="mscs:kql",
        validator=None,
    ),
    field.RestField(
        "index_stats",
        required=False,
        encrypted=False,
    ),
    field.RestField(
        "index_empty_values",
        required=False,
        encrypted=False,
    ),
    field.RestField("disabled", required=False, validator=None),
]
model = RestModel(fields, name=None)


endpoint = DataInputModel(
    "mscs_i2_azure_kql",
    model,
)
