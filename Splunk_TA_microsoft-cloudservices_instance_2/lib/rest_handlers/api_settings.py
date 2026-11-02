#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
from splunktaucclib.rest_handler.endpoint import (
    field,
    RestModel,
    SingleModel,
)

fields = [
    field.RestField(
        "url", required=True, encrypted=False, default=None, validator=None
    ),
    field.RestField(
        "api_version", required=True, encrypted=False, default=None, validator=None
    ),
    field.RestField(
        "sourcetype", required=True, encrypted=False, default=None, validator=None
    ),
    field.RestField(
        "instance_view_url",
        required=False,
        encrypted=False,
        default=None,
        validator=None,
    ),
    field.RestField(
        "network_watcher_url",
        required=False,
        encrypted=False,
        default=None,
        validator=None,
    ),
]

model = RestModel(fields, name=None)

endpoint = SingleModel("mscs_api_settings", model, config_name="mscs_api_settings")
