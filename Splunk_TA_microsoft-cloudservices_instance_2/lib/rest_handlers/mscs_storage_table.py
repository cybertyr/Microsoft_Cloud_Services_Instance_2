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

import mscs_consts
from rest_handlers.settings import fields_logging, fields_performance_tuning_settings
import mscs_util


fields = [
    field.RestField(
        "account", required=True, encrypted=False, default=None, validator=None
    ),
    field.RestField(
        "storage_table_type",
        required=True,
        encrypted=False,
        default="storage_table",
        validator=None,
    ),
    field.RestField(
        "table_list", required=True, encrypted=False, default=None, validator=None
    ),
    field.RestField(
        "start_time",
        required=False,
        encrypted=False,
        default=None,
        validator=mscs_util.StartTimeValidator(),
    ),
    field.RestField(
        "collection_interval",
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
        default="mscs:storage:table",
        validator=None,
    ),
    field.RestField(
        "storage_input_help_link",
        required=False,
        encrypted=False,
        default=None,
        validator=None,
    ),
    field.RestField(
        "storage_virtual_metrics_input_help_link",
        required=False,
        encrypted=False,
        default=None,
        validator=None,
    ),
    field.RestField("disabled", required=False, validator=None),
]
fields.extend(map(mscs_util.empty_field, fields_logging))
fields.extend(
    mscs_util.empty_field(field_)
    for field_ in fields_performance_tuning_settings
    if field_.name in mscs_consts.GLOBAL_FIELD_NAMES_STORAGE_TABLE
)

model = RestModel(fields, name=None)


class StorageTableInputHandler(AdminExternalHandler):
    """
    Custom handler to set the default start_time value as 30 days ago and
    check if the account configuration is valid or not.
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


endpoint = DataInputModel(
    "mscs_i2_storage_table",
    model,
)
