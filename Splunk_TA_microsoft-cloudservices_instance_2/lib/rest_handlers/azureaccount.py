#!/usr/bin/python
#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
from solnlib import log
from splunktaucclib.rest_handler.endpoint import (
    field,
    validator,
    RestModel,
    SingleModel,
)
from splunktaucclib.rest_handler.admin_external import AdminExternalHandler
from cattrs import ClassValidationError, transform_error

from splunk_ta_mscs.models import (
    AzureAccountConfig,
    ModelValidationException,
    format_validation_exception,
)
from mscs_util import (
    get_proxy_info_from_endpoint,
)
from splunk_ta_mscs.mscs_credential_provider import validate_credential
import mscs_consts


_DEFAULT_AUTH_FAILURE_MSG = (
    "Account authentication failed. Please check your credentials and try again"
)


def _format_model_error(exc):
    """Return a user-facing string from a model-validation exception."""
    if isinstance(exc, ClassValidationError):
        messages = transform_error(exc, format_exception=format_validation_exception)
        # Strip the trailing "@ $..." path noise cattrs appends to each message.
        cleaned = [m.split(" @ ")[0] for m in messages if m]
        if cleaned:
            return "; ".join(cleaned)
    return str(exc) or _DEFAULT_AUTH_FAILURE_MSG


class AzureAccountValidation(validator.Validator):
    def __init__(self):

        super(AzureAccountValidation, self).__init__()
        self.put_msg(_DEFAULT_AUTH_FAILURE_MSG)
        self.proxy_config = None

    def validate(self, value, data):
        """Validate account config (model invariants, then network credential check)."""

        logger = log.Logs().get_logger(
            "splunk_ta_microsoft-cloudservices_instance_2_azure_account_validation"
        )
        logger.info("Verifying credentials for MSCS Azure account")
        try:
            account = AzureAccountConfig.from_dict(data)
        except (ClassValidationError, ModelValidationException, ValueError) as e:
            error_msg = _format_model_error(e)
            logger.error(
                f'message="Failed to validate Azure Account model. Error details: {error_msg}"',
                exc_info=e,
            )
            self.put_msg(error_msg)
            return False

        logger.info("Getting proxy details")
        try:
            self.proxy_config = get_proxy_info_from_endpoint()

        except Exception as e:
            logger.error(f'message="Error {e} while getting proxy details"', exc_info=e)
            self.put_msg(_DEFAULT_AUTH_FAILURE_MSG)
            return False

        try:
            validate_credential(account, self.proxy_config.proxy_dict, logger)
        except Exception as e:
            logger.error(
                f'message="Error {e} while verifying the credentials"', exc_info=e
            )
            self.put_msg(_DEFAULT_AUTH_FAILURE_MSG)
            return False

        logger.info("Credentials validated successfully")
        return True


class AzureAccountHandler(AdminExternalHandler):
    """
    This handler is to check if the account configurations are valid or not
    """

    def __init__(self, *args, **kwargs):
        AdminExternalHandler.__init__(self, *args, **kwargs)

    def handleList(self, confInfo):
        AdminExternalHandler.handleList(self, confInfo)

    def handleEdit(self, confInfo):
        AdminExternalHandler.handleEdit(self, confInfo)
        edited_account = next(iter(confInfo))

    def handleCreate(self, confInfo):
        AdminExternalHandler.handleCreate(self, confInfo)
        edited_account = next(iter(confInfo))


fields = [
    field.RestField(
        "client_id", required=True, encrypted=False, default=None, validator=None
    ),
    field.RestField(
        "auth_type",
        required=False,
        encrypted=False,
        default="client_secret",
        validator=None,
    ),
    field.RestField(
        "change_client_secret",
        required=False,
        encrypted=False,
        default=0,
        validator=None,
    ),
    field.RestField(
        "client_secret", required=False, encrypted=True, default=None, validator=None
    ),
    field.RestField(
        "change_certificate_credentials",
        required=False,
        encrypted=False,
        default=0,
        validator=None,
    ),
    field.RestField(
        "certificate_private_key",
        required=False,
        encrypted=True,
        default=None,
        validator=None,
    ),
    field.RestField(
        "certificate_password",
        required=False,
        encrypted=True,
        default=None,
        validator=None,
    ),
    field.RestField(
        "tenant_id",
        required=True,
        encrypted=False,
        default=None,
        validator=AzureAccountValidation(),
    ),
    field.RestField(
        "account_class_type",
        required=True,
        encrypted=False,
        default="1",
        validator=None,
    ),
    field.RestField(
        "app_account_help_link",
        required=False,
        encrypted=False,
        default=None,
        validator=None,
    ),
    field.RestField("disabled", required=False, validator=None),
]

model = RestModel(fields, name=None)

endpoint = SingleModel("mscs_azure_accounts", model, config_name="azureaccount")
