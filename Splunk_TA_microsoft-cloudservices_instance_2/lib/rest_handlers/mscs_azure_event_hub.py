#!/usr/bin/python
#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
from splunktaucclib.rest_handler.endpoint import (
    field,
    validator,
    RestModel,
    DataInputModel,
)
from splunktaucclib.rest_handler.admin_external import AdminExternalHandler
import mscs_util


fields = [
    field.RestField(
        "account",
        required=True,
        encrypted=False,
        default=None,
        validator=None,
    ),
    field.RestField(
        "event_hub_namespace",
        required=True,
        encrypted=False,
        default=None,
        validator=None,
    ),
    field.RestField(
        "event_hub_name",
        required=True,
        encrypted=False,
        default=None,
        validator=None,
    ),
    field.RestField(
        "consumer_group",
        required=False,
        encrypted=False,
        default=None,
        validator=None,
    ),
    field.RestField(
        "max_wait_time",
        required=False,
        encrypted=False,
        default=None,
        validator=None,
    ),
    field.RestField(
        "max_batch_size",
        required=False,
        encrypted=False,
        default=None,
        validator=None,
    ),
    field.RestField(
        "use_amqp_over_websocket",
        required=False,
        encrypted=False,
        default=None,
        validator=None,
    ),
    field.RestField(
        "force_amqp_over_proxy",
        required=False,
        encrypted=False,
        default=False,
        validator=mscs_util.BoolValidator,
    ),
    field.RestField(
        "ensure_ascii",
        required=False,
        encrypted=False,
        default="0",
        validator=mscs_util.BoolValidator,
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
        "sourcetype",
        required=True,
        encrypted=False,
        default="mscs:azure:eventhub:event",
        validator=None,
    ),
    field.RestField(
        "blob_checkpoint_enabled",
        required=False,
        encrypted=False,
        default=False,
        validator=None,
    ),
    field.RestField(
        "storage_account",
        required=False,
        default=None,
        validator=None,
    ),
    field.RestField(
        "container_name",
        required=False,
        encrypted=False,
        default=None,
        validator=None,
    ),
    field.RestField(
        "disabled",
        required=False,
        validator=None,
    ),
    field.RestField(
        "export_status",
        required=False,
        validator=None,
        default=None,
    ),
]


class AzureEventHubInputHandler(AdminExternalHandler):
    """
    Custom handler to check if the account configuration is valid or not
    """

    def handleCreate(self, confInfo):
        status = True

        if (
            not "disabled" in self.callerArgs
            and not "enabled" in self.callerArgs
            and "blob_checkpoint_enabled" in self.callerArgs
        ):
            if (
                self.callerArgs["blob_checkpoint_enabled"] == "1"
                or self.callerArgs["blob_checkpoint_enabled"][0] == "1"
            ):
                status = mscs_util.check_account_secret_isvalid(
                    confInfo,
                    self.getSessionKey(),
                    account_type="storage",
                    storage_account=self.callerArgs["storage_account"],
                )
        if status:
            AdminExternalHandler.handleCreate(self, confInfo)
        else:
            raise ValueError("The account_secret_type NONE_SECRET is not supported.")

    def handleList(self, confInfo):
        AdminExternalHandler.handleList(self, confInfo)

    def handleEdit(self, confInfo):
        status = True
        if (
            not "disabled" in self.callerArgs
            and not "enabled" in self.callerArgs
            and "blob_checkpoint_enabled" in self.callerArgs
        ):
            if (
                self.callerArgs["blob_checkpoint_enabled"] == "1"
                or self.callerArgs["blob_checkpoint_enabled"][0] == "1"
            ):
                status = mscs_util.check_account_secret_isvalid(
                    confInfo,
                    self.getSessionKey(),
                    account_type="storage",
                    storage_account=self.callerArgs["storage_account"],
                )
        if status:
            AdminExternalHandler.handleEdit(self, confInfo)
        else:
            raise ValueError("The account_secret_type NONE_SECRET is not supported.")


model = RestModel(fields, name=None)

endpoint = DataInputModel(
    "mscs_i2_azure_event_hub",
    model,
)
