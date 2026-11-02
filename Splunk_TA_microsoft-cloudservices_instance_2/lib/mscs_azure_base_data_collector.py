#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
import abc

from cattrs import ClassValidationError, transform_error
import mscs_api_error as mae
import mscs_consts
import mscs_logger as logger
import mscs_util
from splunk_ta_mscs.models import (
    AzureAccountConfig,
    format_validation_exception,
    ProxyConfig,
)
import typing as t
from splunk_ta_mscs.mscs_service_client import ServiceClient
from azure.core.rest import HttpRequest
from splunk_ta_mscs.mscs_credential_provider import get_credential
from mscs_request_retrier import RequestRetrier


class AzureBaseDataCollector(metaclass=abc.ABCMeta):
    _NEXT_LINK = "nextLink"

    def __init__(self, all_conf_contents, task_config):
        self._all_conf_contents = all_conf_contents
        self._task_config = task_config

        self._subscription_id = task_config.get(mscs_consts.SUBSCRIPTION_ID)

        self._url = None
        self._api_version = None
        self._logger_prefix = None
        self._sourcetype = None
        self._logger = logger.logger_for(self._get_logger_prefix())

        self._global_settings = all_conf_contents.get(mscs_consts.GLOBAL_SETTINGS, {})
        proxy_settings = self._global_settings.get(mscs_consts.PROXY, {})
        self._proxies = ProxyConfig.from_dict(proxy_settings)

        self._account = self._get_account_from_all_confs()
        self._logger.info(
            f"{self._account.class_type.cloud_environment.name} selected as cloud environment"
        )

        self._manager_url = (
            self._account.class_type.cloud_environment.endpoints.resource_manager
        )
        credential_scopes = self._manager_url + "/.default"

        self._credentials = get_credential(
            self._account, self._proxies.proxy_dict, self._logger
        )

        self._service = ServiceClient(
            self._credentials, self._proxies, scopes=credential_scopes
        )

        tuning_settings = self._global_settings.get(
            mscs_consts.PERFORMANCE_TUNING_SETTINGS, {}
        )
        max_attempts = int(
            mscs_util.find_config_in_settings(
                mscs_consts.MAX_ATTEMPTS,
                mscs_consts.DEFAULT_MAX_ATTEMPTS,
                self._task_config,
                tuning_settings,
            )
        )
        self._request_retrier = RequestRetrier(self._logger, max_attempts=max_attempts)

    @abc.abstractmethod
    def collect_data(self):
        pass

    @abc.abstractmethod
    def _get_logger_prefix(self):
        pass

    def _get_account_from_all_confs(self) -> AzureAccountConfig:
        try:
            account_name = self._task_config[mscs_consts.ACCOUNT]
            account_info = self._all_conf_contents[mscs_consts.ACCOUNTS][account_name]
            return AzureAccountConfig.from_dict(account_info)
        except KeyError as e:
            self._logger.error('message="Failed to read config files"', exc_info=e)
            raise e
        except ClassValidationError as e:
            self._logger.error(
                f'message="Failed to validate Azure Account model for the account: {account_name}. Error details: {transform_error(e, format_exception=format_validation_exception)}"',
                exc_info=e,
            )
            raise e

    def _parse_api_setting(self, api_stanza_name) -> t.Dict:
        api_setting = self._all_conf_contents[mscs_consts.API_SETTINGS][api_stanza_name]
        self._url = api_setting[mscs_consts.URL]
        self._api_version = api_setting[mscs_consts.API_VERSION]
        self._sourcetype = api_setting.get(mscs_consts.SOURCETYPE)
        return api_setting

    def _perform_request(
        self, url, method: str = "GET", payload: dict = None, **headers
    ) -> dict:
        """Performs http request with retry logic for throttling

        Args:
            url (str): API URL
            method (str) : HTTP method
            payload (dict): Dict of API parameters
            headers: Headers required for the API

        Raises:
            Raises an exception in case of any error

        Returns:
            dict: Returns json response dict
        """
        try:

            def send_request():
                request = HttpRequest(method, url, headers=headers, json=payload)
                return self._service.send(request)

            response = self._request_retrier.execute_with_retry(send_request)
            # requests.Response looking object
            result = response.json()
            self._error_check(response, result)
            return result
        except Exception as e:
            self._logger.exception(
                f'message="Error occurred in request url={url}"', exc_info=e
            )
            raise

    @staticmethod
    def _error_check(response, result):
        if response.status_code != 200 or (result and result.get("error")):
            err = result.get("error", {})
            code = err.get("code", result)
            message = err.get("message", result)
            raise mae.APIError(response.status_code, code, message)
