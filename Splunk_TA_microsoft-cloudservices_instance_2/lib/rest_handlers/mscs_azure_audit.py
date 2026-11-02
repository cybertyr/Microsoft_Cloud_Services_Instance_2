#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
from splunktaucclib.rest_handler.endpoint import (
    field,
    validator,
    RestModel,
    SingleModel,
)
from splunktaucclib.rest_handler.admin_external import AdminExternalHandler
import mscs_util


fields = [
    field.RestField(
        "account", required=True, encrypted=False, default=None, validator=None
    ),
    field.RestField(
        "subscription_id", required=True, encrypted=False, default=None, validator=None
    ),
    field.RestField(
        "start_time",
        required=False,
        encrypted=False,
        default=None,
        validator=mscs_util.StartTimeValidator(),
    ),
    field.RestField(
        "interval",
        required=True,
        encrypted=False,
        default="3600",
        validator=validator.Number(
            max_val=31536000,
            min_val=1,
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
        "max_attempts",
        required=False,
        encrypted=False,
        default=None,
        validator=validator.Number(
            max_val=100,
            min_val=1,
        ),
    ),
    field.RestField(
        "audit_help_link", required=False, encrypted=False, default=None, validator=None
    ),
    field.RestField("disabled", required=False, validator=None),
]
model = RestModel(fields, name=None)


class AzureAuditInputHandler(AdminExternalHandler):
    """
    Custom handler to set the default start_time value as 30 days ago and
    check if the account configuration is valid or not
    """

    def update_start_time(self):
        if not self.payload.get("start_time"):
            self.payload["start_time"] = mscs_util.get_30_days_ago_local_time(
                self.getSessionKey()
            )

    def handleList(self, confInfo):
        AdminExternalHandler.handleList(self, confInfo)

    def handleCreate(self, confInfo):
        self.update_start_time()
        AdminExternalHandler.handleCreate(self, confInfo)

    def handleEdit(self, confInfo):
        self.update_start_time()
        AdminExternalHandler.handleEdit(self, confInfo)


endpoint = SingleModel("mscs_azure_audit_inputs", model, config_name="mscs_i2_azure_audit")
