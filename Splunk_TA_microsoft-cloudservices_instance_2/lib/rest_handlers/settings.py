#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
from attr import fields as a_fields
from cattrs import ClassValidationError
from solnlib import log
from splunktaucclib.rest_handler.admin_external import AdminExternalHandler
from splunktaucclib.rest_handler.endpoint.validator import ValidationFailed
from splunktaucclib.rest_handler.endpoint import (
    field,
    validator,
    RestModel,
    MultipleModel,
)

from splunk_ta_mscs.models import ProxyConfig
from rest_handlers.model_validators import RestFieldValidator
from splunk_ta_mscs.global_settings import GlobalSettings


def validate_field_func(field_, attr_class):
    def check_field(value, data):
        if field_.type == "int":
            try:
                value = int(value)
            except ValueError as ve:
                msg = (
                    f"Integer value expected. Provided value {value} is not an integer."
                )
                raise ValidationFailed(msg) from ve

        return field_.validator(attr_class, field_, value)

    return check_field


class ProxyValidation(validator.Validator):
    logger = log.Logs().get_logger("splunk_ta_microsoft-cloudservices_instance_2_settings")

    def __init__(self, *args, **kwargs):
        super(ProxyValidation, self).__init__(*args, **kwargs)

    def validate_model(self, payload) -> bool:
        try:
            ProxyConfig.from_dict(payload)
        except ClassValidationError as e:
            inner_exceptions = " ,".join(map(str, e.exceptions))
            self.logger.error(
                f'message="Error in validating ProxyConfig: {e}. Inner exceptions: {inner_exceptions}"',
                exc_info=e,
            )
            self.put_msg("Error in validating proxy configuration", high_priority=True)
            return False

        return True

    def validate(self, value, data):
        username = data.get("proxy_username")
        password = data.get("proxy_password")

        if password and not username:
            self.put_msg(
                "Username is required if password is specified", high_priority=True
            )
            return False

        return self.validate_model(data)


fields_proxy = [
    field.RestField(
        "proxy_enabled", required=False, encrypted=False, default=None, validator=None
    ),
    field.RestField(
        "proxy_type", required=True, encrypted=False, default="http", validator=None
    ),
    field.RestField(
        "proxy_rdns", required=False, encrypted=False, default=None, validator=None
    ),
    field.RestField(
        "proxy_url",
        required=True,
        encrypted=False,
        default=None,
        validator=validator.AllOf(
            validator.String(
                max_len=4096,
                min_len=0,
            ),
            validator.Pattern(
                regex=r"""^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9])\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9-]*[A-Za-z0-9])$""",
            ),
        ),
    ),
    field.RestField(
        "proxy_port",
        required=True,
        encrypted=False,
        default=None,
        validator=validator.Number(
            max_val=65535,
            min_val=1,
        ),
    ),
    field.RestField(
        "proxy_username",
        required=False,
        encrypted=False,
        default=None,
        validator=validator.String(
            max_len=50,
            min_len=0,
        ),
    ),
    field.RestField(
        "proxy_password",
        required=False,
        encrypted=True,
        default=None,
        validator=ProxyValidation(),
    ),
]
model_proxy = RestModel(fields_proxy, name="proxy")

fields_logging = [
    field.RestField(
        "agent", required=False, encrypted=False, default="INFO", validator=None
    )
]
model_logging = RestModel(fields_logging, name="logging")

fields_performance_tuning_settings = [
    field.RestField(
        name=field_.name,
        default=str(field_.default),
        required=False,
        encrypted=False,
        validator=RestFieldValidator(validate_field_func(field_, GlobalSettings)),
    )
    for field_ in a_fields(GlobalSettings)
]

model_performance_tuning_settings = RestModel(
    fields_performance_tuning_settings, name="performance_tuning_settings"
)
endpoint = MultipleModel(
    "splunk_ta_mscs_i2_settings",
    models=[
        model_proxy,
        model_logging,
        model_performance_tuning_settings,
    ],
)


class CustomMSCSSettingsHandler(AdminExternalHandler):
    """
    Custom Handler to support handleList for MultiModel if no callerArgs is provided
    """

    CONF_MODEL_LIST = ["logging", "proxy", "performance_tuning_settings"]

    def handleList(self, confInfo):
        if self.callerArgs.id:
            setting_models = [self.callerArgs.id]
        else:
            setting_models = self.CONF_MODEL_LIST

        for each_model in setting_models:
            self.callerArgs.id = each_model
            AdminExternalHandler.handleList(self, confInfo)
