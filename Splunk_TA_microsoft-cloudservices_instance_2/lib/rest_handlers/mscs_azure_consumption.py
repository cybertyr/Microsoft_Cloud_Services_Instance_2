#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
from splunk import admin
from splunktaucclib.rest_handler.endpoint import (
    field,
    validator,
    RestModel,
    DataInputModel,
)
from splunktaucclib.rest_handler.admin_external import AdminExternalHandler
from datetime import datetime, timedelta, timezone
from mscs_common_utils import set_logger
from mscs_base_data_collector import AzureBaseDataCollector as base_data_collector
import mscs_common_api_error as mae
import traceback
import mscs_util

VALIDATION_RETRIES = 0


class GetSessionKey(admin.MConfigHandler):
    def __init__(self):
        self.session_key = self.getSessionKey()


class MscsAzureConsumptionValidator(validator.Validator):
    def __init__(self):
        super(MscsAzureConsumptionValidator, self).__init__()

    def validate(self, value, data):
        self.log_file_name = (
            "splunk_ta_microsoft-cloudservices_instance_2_rh_mscs_azure_consumption"
        )
        self.logger = set_logger(GetSessionKey().session_key, self.log_file_name)
        try:
            self.base_data_collector_object = base_data_collector(
                logger=self.logger,
                session_key=GetSessionKey().session_key,
                account_name=data.get("account"),
            )
        except Exception as e:
            self.put_msg(
                "Account authentication failed. Please check your credentials and try again."
            )
            self.logger.error(
                f'message="Error occured while verifying the credentials: {e}"',
                exc_info=e,
            )
            return False
        self.subscription_id = data.get("subscription_id")
        self.data_type = data.get("data_type")
        if self.data_type == "Usage Details":
            return self.validate_usage_details_input(data)
        elif self.data_type == "Reservation Recommendation":
            return self.validate_reservation_recommendation_input()

    def prepare_response(self, **kwargs):
        url = self.base_data_collector_object._url.format(
            api_version=self.base_data_collector_object._api_version,
            subscription_id=self.subscription_id,
            base_host=self.base_data_collector_object._manager_url,
        )
        try:
            self.base_data_collector_object._perform_request(url, **kwargs)
        except mae.APIError as e:
            if e.status == 204 and e.error_msg == "No Content":
                return True
            self.put_msg(e.error_msg)
            return False
        return True

    def validate_usage_details_input(self, data):
        start_date = data.get("start_date") or (
            datetime.now(timezone.utc) - timedelta(90)
        ).strftime("%Y-%m-%d")
        try:
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
            past_date = datetime.strptime("2014-05-01", "%Y-%m-%d").date()
            now = datetime.now(timezone.utc).date()
            if start_date < past_date:
                self.put_msg("'Start Date' cannot be older than May 1, 2014")
                return False
            if start_date > now:
                self.put_msg("'Start Date' cannot be in the future")
                return False

        except ValueError:
            self.put_msg(
                "Invalid 'Start Date'. Please enter a valid 'Start Date' in the YYYY-MM-DD format."
            )
            return False
        self.base_data_collector_object._parse_api_setting("usage_details")
        try:
            query_days = int(data.get("query_days") or 10)
            end_date = start_date + timedelta(days=query_days)
            date_days_ago = datetime.now(timezone.utc).date() - timedelta(days=1)
        except ValueError:
            self.put_msg(
                "Max days to query must be a non-zero positive integer. Defaults to 10 days."
            )
            return False
        except Exception:
            self.put_msg(
                "The maximum query window size for this operation has been exceeded. Please try a smaller time period."
            )
            return False
        if end_date > date_days_ago:
            end_date = date_days_ago
        params = {
            "$orderby": "properties/usageEnd",
            "$expand": "properties/meterDetails,properties/additionalInfo",
            "$filter": "properties/usageStart ge '{}' AND properties/usageEnd le '{}'".format(
                start_date, end_date
            ),
        }
        return self.prepare_response(params=params, retries=VALIDATION_RETRIES)

    def validate_reservation_recommendation_input(self):
        self.base_data_collector_object._parse_api_setting("reservation_recommendation")
        return self.prepare_response(retries=VALIDATION_RETRIES)


fields = [
    field.RestField(
        "account", required=True, encrypted=False, default=None, validator=None
    ),
    field.RestField(
        "subscription_id",
        required=True,
        encrypted=False,
        default=None,
        validator=MscsAzureConsumptionValidator(),
    ),
    field.RestField(
        "interval",
        required=True,
        encrypted=False,
        default="86400",
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
        default="default",
        validator=mscs_util.MscsAzureIndexValidator(),
    ),
    field.RestField(
        "sourcetype",
        required=True,
        encrypted=False,
        default="mscs:consumption:billing",
    ),
    field.RestField(
        "data_type",
        required=True,
        encrypted=False,
        default="Usage Details",
        validator=None,
    ),
    field.RestField(
        "query_days",
        required=False,
        encrypted=False,
        default="10",
        validator=validator.Pattern(
            regex=r"""^[1-9]\d*$""",
        ),
    ),
    field.RestField(
        "start_date",
        required=False,
        encrypted=False,
        default=None,
        validator=None,
    ),
    field.RestField("disabled", required=False, validator=None),
]
model = RestModel(fields, name=None)


endpoint = DataInputModel(
    "mscs_i2_azure_consumption",
    model,
)


class MscsAzureConsumptionExternalHandler(AdminExternalHandler):
    def __init__(self, *args, **kwargs):
        AdminExternalHandler.__init__(self, *args, **kwargs)

    def checkStartDate(self):
        # Check if start_date field is empty.
        # If so, set its default value to three months ago so that it gets reflected in UI.
        default_date = datetime.now(timezone.utc) - timedelta(90)
        data_type = self.payload.get("data_type")
        if data_type == "Usage Details":
            if not self.payload.get("start_date"):
                self.payload["start_date"] = datetime.strftime(default_date, "%Y-%m-%d")

    def handleEdit(self, conf_info):
        AdminExternalHandler.handleEdit(self, conf_info)

    def handleCreate(self, conf_info):
        self.checkStartDate()
        AdminExternalHandler.handleCreate(self, conf_info)
