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
from rest_handlers.mscs_storage_blob_validators import CompressionTypeValidator

from rest_handlers.settings import fields_logging, fields_performance_tuning_settings
import mscs_consts
import mscs_util


fields = [
    field.RestField(
        mscs_consts.ACCOUNT,
        required=True,
        encrypted=False,
        default=None,
        validator=None,
    ),
    field.RestField(
        mscs_consts.CONTAINER_NAME,
        required=True,
        encrypted=False,
        default=None,
        validator=validator.AllOf(
            validator.Pattern(
                regex=r"""^.{3,63}$""",
            ),
            validator.Pattern(
                regex=r"""^[0-9a-z-]*$""",
            ),
            validator.Pattern(
                regex=r"""^(?!.*--)[^-].*[^-]$""",
            ),
        ),
    ),
    field.RestField(
        mscs_consts.PREFIX,
        required=False,
        encrypted=False,
        default=None,
        validator=validator.String(
            max_len=4000,
            min_len=0,
        ),
    ),
    field.RestField(
        mscs_consts.BLOB_LIST,
        required=False,
        encrypted=False,
        default=None,
        validator=None,
    ),
    field.RestField(
        mscs_consts.BLOB_MODE,
        required=True,
        encrypted=False,
        default=mscs_consts.BLOB_MODE_RANDOM,
        validator=validator.Enum(values=mscs_consts.BLOB_MODE_LIST),
    ),
    field.RestField(
        mscs_consts.BLOB_COMPRESSION,
        required=False,
        encrypted=False,
        default=mscs_consts.BLOB_NOT_COMPRESSED,
        validator=CompressionTypeValidator(),
    ),
    field.RestField(
        mscs_consts.PARSE_CSV_WITH_HEADER,
        required=False,
        encrypted=False,
        default=False,
        validator=None,
    ),
    field.RestField(
        mscs_consts.PARSE_CSV_WITH_DELIMITER,
        required=False,
        encrypted=False,
        default=",",
        validator=validator.String(
            max_len=2,
            min_len=1,
        ),
    ),
    field.RestField(
        mscs_consts.DONT_REUPLOAD_BLOB_SAME_SIZE,
        required=False,
        encrypted=False,
        default=0,
        validator=None,
    ),
    field.RestField(
        mscs_consts.IS_MIGRATED,
        required=False,
        encrypted=False,
        default="0",
        validator=None,
    ),
    field.RestField(
        mscs_consts.EXCLUDE_BLOB_LIST,
        required=False,
        encrypted=False,
        default=None,
        validator=None,
    ),
    field.RestField(
        mscs_consts.LOG_TYPE,
        required=False,
        encrypted=False,
        default=None,
        validator=None,
    ),
    field.RestField(
        mscs_consts.GUIDS, required=False, encrypted=False, default=None, validator=None
    ),
    field.RestField(
        mscs_consts.APPLICATION_INSIGHTS,
        required=False,
        encrypted=False,
        default=None,
        validator=None,
    ),
    field.RestField(
        mscs_consts.DECODING,
        required=False,
        encrypted=False,
        default=None,
        validator=validator.Pattern(
            regex=r"""^[a-zA-Z0-9][a-zA-Z0-9\_\-]*$""",
        ),
    ),
    field.RestField(
        mscs_consts.COLLECTION_INTERVAL,
        required=True,
        encrypted=False,
        default="3600",
        validator=validator.Number(
            max_val=31536000,
            min_val=1,
        ),
    ),
    field.RestField(
        mscs_consts.INDEX,
        required=True,
        encrypted=False,
        default="default",
        validator=mscs_util.MscsAzureIndexValidator(),
    ),
    field.RestField(
        mscs_consts.SOURCETYPE,
        required=True,
        encrypted=False,
        default="mscs:storage:blob",
        validator=None,
    ),
    field.RestField(
        mscs_consts.BLOB_INPUT_HELP_LINK,
        required=False,
        encrypted=False,
        default=None,
        validator=None,
    ),
    field.RestField(mscs_consts.DISABLED, required=False, validator=None),
    field.RestField(
        mscs_consts.READ_TIMEOUT,
        required=False,
        encrypted=False,
        default="60",
        validator=validator.AllOf(
            validator.Pattern(
                regex=r"""^[1-9]\d*$""",
            ),
            validator.Number(
                max_val=80000,
                min_val=1,
            ),
        ),
    ),
]
fields.extend(map(mscs_util.empty_field, fields_logging))
fields.extend(
    mscs_util.empty_field(field_)
    for field_ in fields_performance_tuning_settings
    if field_.name in mscs_consts.GLOBAL_FIELD_NAMES_STORAGE_BLOB
)


class StorageBlobInputHandler(AdminExternalHandler):
    """
    Custom handler to check if the account configuration is valid or not
    """

    def handleCreate(self, confInfo):
        AdminExternalHandler.handleCreate(self, confInfo)

    def handleList(self, confInfo):
        AdminExternalHandler.handleList(self, confInfo)

    def handleEdit(self, confInfo):
        AdminExternalHandler.handleEdit(self, confInfo)


model = RestModel(fields, name=None)


endpoint = DataInputModel(
    "mscs_i2_storage_blob",
    model,
)
