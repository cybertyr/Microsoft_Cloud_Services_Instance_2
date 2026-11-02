# encoding = utf-8
# Always put this line at the beginning of this file
import import_declare_test

import sys
import traceback

from azure.mgmt.compute import ComputeManagementClient
from mscs_common_utils import get_account_from_config, get_log_level
from mscs_consts import (
    VALUE_ERROR_RETURN_CODE,
    GENERAL_ERROR_RETURN_CODE,
    SUCCESS_RETURN_CODE,
)
from mscs_util import get_proxy_info_from_endpoint
from splunktaucclib.alert_actions_base import ModularAlertBase
from splunk_ta_mscs_i2.mscs_credential_provider import get_credential


class AlertActionStopAzureVM(ModularAlertBase):
    def __init__(self, ta_name, alert_name):
        super(AlertActionStopAzureVM, self).__init__(ta_name, alert_name)

    def _required_args(self):
        """Returns the Configurations required to perform Alert action."""

        return {
            "account": self.get_param("account"),
            "resource_group": self.get_param("resource_group"),
            "subscription_id": self.get_param("subscription_id"),
            "vm_name": self.get_param("vm_name"),
        }

    def _get_account(self):
        """Get the MSCS account stanza from the configuration file."""
        account_stanza = get_account_from_config(
            self._logger, self.session_key, self.account
        )
        return account_stanza

    def _set_client(self, credential, proxy_dict):
        """Get ComputeManagementClient object."""
        self._client = ComputeManagementClient(
            credential=credential,
            subscription_id=self.subscription_id,
            proxies=proxy_dict,
        )

    def _prepare(self):
        """Prepare object to perform the Stop VM operation"""

        # Get and set the Alert configurations
        configs = self._required_args()
        [setattr(self, key, value.strip()) for key, value in configs.items()]

        # Get and set the Splunk Session Key
        account_stanza = self._get_account()

        # Prepare Azure credential object
        proxies = get_proxy_info_from_endpoint(self.session_key)
        credential = get_credential(account_stanza, proxies.proxy_dict, self._logger)
        self._set_client(credential, proxies.proxy_dict)

    def _execute(self):
        self.log_info(
            f"Begin Power off for vm_name={self.vm_name}, resource_group={self.resource_group}"
        )
        self._client.virtual_machines.begin_power_off(
            resource_group_name=self.resource_group,
            vm_name=self.vm_name,
        )
        self.log_info(
            f"Finished Power off for vm_name={self.vm_name}, resource_group={self.resource_group}"
        )

    def get_log_level(self):
        """Override the function of Base class to get the log level"""
        return get_log_level(self.session_key)

    def _validate_params(self):
        """Validates the required parameters."""
        required_args = self._required_args()
        errs = [key for key, val in required_args.items() if not val]
        if errs:
            self.log_error(f"Required arguments are missing: {str(errs)}")
            raise ValueError

    def process_event(self, *args, **kwargs):
        status = SUCCESS_RETURN_CODE
        try:
            self._validate_params()
            self._prepare()
            self._execute()
        except ValueError:
            return VALUE_ERROR_RETURN_CODE
        except Exception as e:
            stacktrace = traceback.format_exc()
            msg = (
                f"Unexpected error occured!: {e}. \n {stacktrace}"
                if str(e) and stacktrace
                else f"Unexpected error occured!: {e}"
            )
            self.log_error(msg)
            return GENERAL_ERROR_RETURN_CODE
        return status


def main():
    exitcode = AlertActionStopAzureVM(
        "Splunk_TA_microsoft-cloudservices_instance_2", "mscs_i2_stop_azure_vm"
    ).run(sys.argv)
    sys.exit(exitcode)
