#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
import re
import unicodedata

from splunktaucclib.rest_handler.endpoint import (
    field,
    validator,
    RestModel,
    SingleModel,
)
from splunktaucclib.rest_handler.admin_external import AdminExternalHandler

import mscs_consts
import mscs_util


class MscsAzureResourceValidator(validator.Validator):
    def __init__(self):
        super(MscsAzureResourceValidator, self).__init__()

    def validate(self, value, data):
        resource_group_str = data["resource_group_list"].strip()

        if resource_group_str:

            resource_group_list = [
                resource_group.strip().lower()
                for resource_group in resource_group_str.split(",")
                if len(resource_group.strip())
            ]

            for resource_group in resource_group_list:
                if resource_group_list.count(resource_group) > 1:
                    self.put_msg(
                        "Field Resource Group List is case insensitive and duplicate resource groups are not allowed."
                    )
                    return False

        return True


class MscsAzureResourceTypeValidator(validator.Validator):
    def __init__(self):
        super(MscsAzureResourceTypeValidator, self).__init__()

    def validate(self, value, data):
        resource_type = data[mscs_consts.RESOURCE_TYPE]

        if resource_type != mscs_consts.SUBSCRIPTION_RESOURCE_TYPE and not data.get(
            mscs_consts.SUBSCRIPTION_ID, None
        ):
            self.put_msg("Field Subscription ID is required")
            return False

        if resource_type == mscs_consts.RESOURCE_GRAPH_RESOURCE_TYPE and not data.get(
            mscs_consts.RESOURCE_GRAPH_QUERY, None
        ):
            self.put_msg("Field Resource Graph Query is required")
            return False

        if resource_type == mscs_consts.TOPOLOGY_RESOURCE_TYPE:
            topology_params = [
                data.get(mscs_consts.NETWORK_WATCHER_NAME),
                data.get(mscs_consts.TARGET_RESOURCE_GROUP),
                data.get(mscs_consts.NETWORK_WATCHER_RESOURCE_GROUP),
            ]
            if any(topology_params) and not all(topology_params):
                self.put_msg(
                    "All three fields are required, if any of the following provided: 'Network Watcher Name', 'Network Watcher Resource Group', or 'Target Resource Group'"
                )
                return False

        return True


class AzureResourceGroupNameValidator(validator.Validator):
    def __init__(self):
        super(AzureResourceGroupNameValidator, self).__init__()

    def _validate_length(self, value):
        if len(value) > 90:
            return False
        return True

    def _validate_rg_name(self, value):
        """
        Validates that the string:
        - Can only contain alphanumeric, underscore (_), parentheses (()), hyphen (-), period (.) (except at the end).
        - Allows Unicode characters from the allowed categories:
        - UppercaseLetter, LowercaseLetter, TitlecaseLetter, ModifierLetter, OtherLetter, DecimalDigitNumber.
        - Does NOT allow spaces.
        - Does NOT allow a period (.) at the end.
        """
        # Regex to ensure the allowed characters (ASCII alphanumeric, underscore, hyphen, period, parentheses)
        pattern = r"^[\w\-.()]*[^.\s]$"
        if not re.match(pattern, value):
            return False

        # Unicode character validation
        for char in value:
            char_type = unicodedata.category(char)
            if (
                char_type not in {"Lu", "Ll", "Lt", "Lm", "Lo", "Nd"}
                and char not in "_-()."
            ):
                return False

        return True

    def validate(self, value, data):
        network_watcher_resource_group = data.get("network_watcher_resource_group")
        target_resource_group = data.get("target_resource_group")

        if network_watcher_resource_group:
            if not self._validate_length(network_watcher_resource_group):
                self.put_msg(
                    f"Maximum length allowed for Network Watcher Resource Group is 90"
                )
                return False

            if not self._validate_rg_name(network_watcher_resource_group):
                self.put_msg(
                    "Network Watcher Resource Group can only include alphanumeric, underscore, parentheses, hyphen, period (except at end), and Unicode characters within the allowed categories (UppercaseLetter, LowercaseLetter, TitlecaseLetter, ModifierLetter, OtherLetter, DecimalDigitNumber)"
                )
                return False

        if target_resource_group:
            if not self._validate_length(target_resource_group):
                self.put_msg(f"Maximum length allowed for Target Resource Group is 90")
                return False

            if not self._validate_rg_name(target_resource_group):
                self.put_msg(
                    "Target Resource Group can only include alphanumeric, underscore, parentheses, hyphen, period (except at end), and Unicode characters within the allowed categories (UppercaseLetter, LowercaseLetter, TitlecaseLetter, ModifierLetter, OtherLetter, DecimalDigitNumber)"
                )
                return False

        return True


fields = [
    field.RestField(
        "account", required=True, encrypted=False, default=None, validator=None
    ),
    field.RestField(
        "subscription_id", required=False, encrypted=False, default=None, validator=None
    ),
    field.RestField(
        "resource_type",
        required=True,
        encrypted=False,
        default="virtual_machine",
        validator=MscsAzureResourceTypeValidator(),
    ),
    field.RestField(
        "resource_group_list",
        required=False,
        encrypted=False,
        default=None,
        validator=MscsAzureResourceValidator(),
    ),
    field.RestField(
        "resource_graph_query",
        required=False,
        encrypted=False,
        default=None,
        validator=validator.AllOf(
            validator.String(
                max_len=8192,
                min_len=1,
            ),
        ),
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
        "resource_help_link",
        required=False,
        encrypted=False,
        default=None,
        validator=None,
    ),
    field.RestField(
        "network_watcher_name",
        required=False,
        encrypted=False,
        default=None,
        validator=validator.AllOf(
            validator.String(
                max_len=80,
                min_len=1,
            ),
            validator.Pattern(
                regex=r"""^[a-zA-Z0-9](?:[a-zA-Z0-9._-]*[a-zA-Z0-9_])?$""",
            ),
        ),
    ),
    field.RestField(
        "network_watcher_resource_group",
        required=False,
        encrypted=False,
        default=None,
        validator=AzureResourceGroupNameValidator(),
    ),
    field.RestField(
        "target_resource_group",
        required=False,
        encrypted=False,
        default=None,
        validator=AzureResourceGroupNameValidator(),
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
    field.RestField("disabled", required=False, validator=None),
]


class AzureResourceInputHandler(AdminExternalHandler):
    """
    Custom handler to set the default start_time value as 30 days ago and
    check if the account configuration is valid or not.
    """

    def _remove_invalid_fields(self):
        resource_type = self.payload.get(mscs_consts.RESOURCE_TYPE)
        if resource_type:
            if (
                resource_type == mscs_consts.SUBSCRIPTION_RESOURCE_TYPE
                and self.payload.get(mscs_consts.SUBSCRIPTION_ID)
            ):
                del self.payload[mscs_consts.SUBSCRIPTION_ID]

            without_resourcegroup_param_types = [
                mscs_consts.SUBSCRIPTION_RESOURCE_TYPE,
                mscs_consts.RESOURCE_GRAPH_RESOURCE_TYPE,
                mscs_consts.TOPOLOGY_RESOURCE_TYPE,
            ]
            if resource_type in without_resourcegroup_param_types and self.payload.get(
                mscs_consts.RESOURCE_GROUP_LIST
            ):
                del self.payload[mscs_consts.RESOURCE_GROUP_LIST]

            if (
                resource_type != mscs_consts.RESOURCE_GRAPH_RESOURCE_TYPE
                and self.payload.get(mscs_consts.RESOURCE_GRAPH_QUERY)
            ):
                del self.payload[mscs_consts.RESOURCE_GRAPH_QUERY]

            if resource_type != mscs_consts.TOPOLOGY_RESOURCE_TYPE:
                if self.payload.get(mscs_consts.NETWORK_WATCHER_NAME):
                    del self.payload[mscs_consts.NETWORK_WATCHER_NAME]
                if self.payload.get(mscs_consts.TARGET_RESOURCE_GROUP):
                    del self.payload[mscs_consts.TARGET_RESOURCE_GROUP]
                if self.payload.get(mscs_consts.NETWORK_WATCHER_RESOURCE_GROUP):
                    del self.payload[mscs_consts.NETWORK_WATCHER_RESOURCE_GROUP]

    def handleList(self, confInfo):
        AdminExternalHandler.handleList(self, confInfo)

    def handleEdit(self, confInfo):
        self._remove_invalid_fields()
        AdminExternalHandler.handleEdit(self, confInfo)

    def handleCreate(self, confInfo):
        self._remove_invalid_fields()
        AdminExternalHandler.handleCreate(self, confInfo)


model = RestModel(fields, name=None)


endpoint = SingleModel(
    "mscs_azure_resource_inputs", model, config_name="mscs_i2_azure_resource"
)
