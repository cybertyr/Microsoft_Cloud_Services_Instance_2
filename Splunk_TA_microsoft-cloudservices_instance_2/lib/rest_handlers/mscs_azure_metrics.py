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

import mscs_util

fields = [
    field.RestField(
        "account", required=True, encrypted=False, default=None, validator=None
    ),
    field.RestField(
        "subscription_id",
        required=True,
        encrypted=False,
        default=None,
        validator=validator.String(
            max_len=8192,
            min_len=1,
        ),
    ),
    field.RestField(
        "namespaces",
        required=True,
        encrypted=False,
        default=None,
        validator=validator.String(
            max_len=8192,
            min_len=1,
        ),
    ),
    field.RestField(
        "metric_statistics",
        required=True,
        encrypted=False,
        default="average",
        validator=None,
    ),
    field.RestField(
        "preferred_time_aggregation",
        required=True,
        encrypted=False,
        default="PT1M",
        validator=None,
    ),
    field.RestField(
        "interval",
        required=True,
        encrypted=False,
        default="300",
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
        "metric_index_flag",
        required=False,
        encrypted=False,
        default="yes",
        validator=None,
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
        default="mscs:metrics",
        validator=None,
    ),
    field.RestField(
        "number_of_threads",
        required=True,
        encrypted=False,
        default="5",
        validator=validator.AllOf(
            validator.Pattern(
                regex=r"""^[1-9]\d*$""",
            ),
            validator.Number(
                max_val=256,
                min_val=1,
            ),
        ),
    ),
    field.RestField("disabled", required=False, validator=None),
]
model = RestModel(fields, name=None)


endpoint = DataInputModel(
    "mscs_i2_azure_metrics",
    model,
)
