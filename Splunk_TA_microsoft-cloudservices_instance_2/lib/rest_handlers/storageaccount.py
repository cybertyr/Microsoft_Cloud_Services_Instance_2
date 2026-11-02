#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
import traceback

from solnlib import log
from cattrs import ClassValidationError, transform_error
from splunktaucclib.rest_handler.admin_external import (  # isort: skip # pylint: disable=import-error
    AdminExternalHandler,
)

from splunk_ta_mscs_i2.models import (
    AzureStorageAccountConfig,
    format_validation_exception,
    NoneSecretType,
)
from mscs_storage_service import (  # isort:skip # pylint: disable=import-error
    _create_blob_service,
    _create_table_service,
)
from mscs_util import (  # isort:skip # pylint: disable=import-error
    get_proxy_info_from_endpoint,
)
from splunktaucclib.rest_handler.endpoint import (  # isort:skip # pylint: disable=import-error
    RestModel,
    SingleModel,
    field,
    validator,
)


class StorageAccountValidation(validator.Validator):
    def __init__(self):
        super(StorageAccountValidation, self).__init__()
        self.put_msg(
            "Storage Account authentication failed. Please check your credentials and try again"
        )
        self.proxy_config = None

    def validate(self, value, data):
        """Validate the given values.

        :param value: Value of particular parameter
        :param data: Whole payload
        :return: True if validation pass, False otherwise.
        """

        logger = log.Logs().get_logger(
            "splunk_ta_microsoft-cloudservices_instance_2_storage_account_validation"
        )

        logger.info("Verifying credentials for the MSCS Storage account")
        try:
            storage_account = AzureStorageAccountConfig.from_dict(data)
        except ClassValidationError as e:
            logger.error(
                f'message="Failed to validate Azure Storage Account model. Error details: {transform_error(e, format_exception=format_validation_exception)}"',
                exc_info=e,
            )
            return False

        if storage_account.secret_type == NoneSecretType():
            return True

        logger.info("Getting proxy details")
        try:
            self.proxy_config = get_proxy_info_from_endpoint()
        except Exception as e:
            logger.error(f'message="Error {e} while getting proxy details"', exc_info=e)
            return False

        try:
            # Create the object of class BlobServiceClient and TableServiceClient
            blob_service_client = _create_blob_service(
                storage_account=storage_account,
                proxies=self.proxy_config.proxy_dict,
            )
            table_service = _create_table_service(
                storage_account=storage_account,
                proxies=self.proxy_config.proxy_dict,
            )

            # Verify the credentials, pass num_results=1 so that it does not return much data
            paged_containers = blob_service_client.list_containers(results_per_page=1)
            paged_containers.next()
            paged_tables = table_service.list_tables(results_per_page=1)
            paged_tables.next()
        except StopIteration:
            pass
        except Exception as e:
            logger.error(
                f'message="Error {e} while verifying the credentials"',
                exc_info=e,
            )
            return False

        logger.info("Credentials validated successfully")
        return True


class StorageAccountHandler(AdminExternalHandler):
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
        "account_name", required=True, encrypted=False, default=None, validator=None
    ),
    field.RestField(
        "account_secret", required=False, encrypted=True, default=None, validator=None
    ),
    field.RestField(
        "account_secret_type",
        required=True,
        encrypted=False,
        default="1",
        validator=None,
    ),
    field.RestField(
        "account_class_type",
        required=True,
        encrypted=False,
        default="1",
        validator=StorageAccountValidation(),
    ),
    field.RestField(
        "storage_account_help_link",
        required=False,
        encrypted=False,
        default=None,
        validator=None,
    ),
    field.RestField("disabled", required=False, validator=None),
]

model = RestModel(fields, name=None)

endpoint = SingleModel("mscs_storage_accounts", model, config_name="storageaccount")
