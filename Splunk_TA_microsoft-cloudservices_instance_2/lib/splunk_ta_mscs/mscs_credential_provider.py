#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#

from logging import Logger
from typing import Optional, Union

from azure.core.pipeline.policies import RetryPolicy
from azure.identity import CertificateCredential, ClientSecretCredential

import mscs_consts
from splunk_ta_mscs.models import AzureAccountConfig

TokenCredential = Union[ClientSecretCredential, CertificateCredential]


def _build_logging_retry_policy(logger: Optional[Logger]):
    # azure-core's RetryPolicy retries silently; overriding increment() is the one hook to log attempts.
    class LoggingRetryPolicy(RetryPolicy):
        def increment(self, settings, response=None, error=None):
            attempts_used = self.total_retries - settings["total"]
            max_attempts = self.total_retries + 1
            if logger:
                if error is not None:
                    error_desc = str(error)
                elif hasattr(response, "http_response"):
                    error_desc = f"HTTP {response.http_response.status_code}"
                else:
                    error_desc = "unknown error"
                logger.warning(
                    f'message="Token request failed" '
                    f'attempt={attempts_used + 1}/{max_attempts} error="{error_desc}"'
                )
            will_retry = super().increment(settings, response=response, error=error)
            if not will_retry and logger:
                logger.error(
                    f'message="Token request will not be retried" attempts={attempts_used + 1}/{max_attempts}'
                )
            return will_retry

    return LoggingRetryPolicy


def get_credential(
    account: AzureAccountConfig, proxies: dict, logger: Logger = None
) -> TokenCredential:
    if logger:
        logger.debug(
            f'message="Building credential object" auth_type={account.auth_type} tenant_id={account.tenant_id} authority={account.class_type.authority_url}'
        )
    if account.auth_type == mscs_consts.AUTH_TYPE_CLIENT_CERTIFICATE:
        return _get_certificate_credential(account, proxies, logger)
    return _get_secret_credential(account, proxies, logger)


def _get_secret_credential(
    account: AzureAccountConfig, proxies: dict, logger: Logger = None
) -> ClientSecretCredential:
    return ClientSecretCredential(
        client_id=str(account.client_id),
        client_secret=account.client_secret,
        tenant_id=str(account.tenant_id),
        authority=account.class_type.authority_url,
        proxies=proxies,
        connection_timeout=mscs_consts.TOKEN_REQUEST_CONNECTION_TIMEOUT_SEC,
        read_timeout=mscs_consts.TOKEN_REQUEST_READ_TIMEOUT_SEC,
        retry_total=mscs_consts.TOKEN_REQUEST_RETRY_TOTAL,
        retry_policy_class=_build_logging_retry_policy(logger),
    )


def _get_certificate_credential(
    account: AzureAccountConfig, proxies: dict, logger: Logger = None
) -> CertificateCredential:
    # `certificate_data` expects a full PEM bundle (cert + private key).
    # CertificateCredential derives the thumbprint internally from the cert material.
    certificate_pem = account.certificate_private_key.encode("utf-8")
    password = (
        account.certificate_password.encode("utf-8")
        if account.certificate_password
        else None
    )
    return CertificateCredential(
        client_id=str(account.client_id),
        tenant_id=str(account.tenant_id),
        certificate_data=certificate_pem,
        password=password,
        authority=account.class_type.authority_url,
        proxies=proxies,
        send_certificate_chain=True,
        connection_timeout=mscs_consts.TOKEN_REQUEST_CONNECTION_TIMEOUT_SEC,
        read_timeout=mscs_consts.TOKEN_REQUEST_READ_TIMEOUT_SEC,
        retry_total=mscs_consts.TOKEN_REQUEST_RETRY_TOTAL,
        retry_policy_class=_build_logging_retry_policy(logger),
    )


def validate_credential(account: AzureAccountConfig, proxies: dict, logger: Logger):
    try:
        credential = get_credential(account, proxies, logger)
        scope = f"{account.class_type.cloud_environment.endpoints.resource_manager}/.default"
        credential.get_token(scope)
    except Exception as e:
        logger.error('message="Credential validation failed"', exc_info=e)
        raise

    _check_account_class_type(credential, account.class_type.authority_url, logger)


def _check_account_class_type(
    credential: TokenCredential, authority_url: str, logger: Logger
):
    authorization_endpoint = credential._get_app().authority.authorization_endpoint
    try:
        authorization_endpoint.index(authority_url)
    except ValueError as e:
        logger.error(
            f'message="Actual authorization endpoint ({authorization_endpoint}) does not match expected authority ({authority_url})"',
            exc_info=e,
        )
        raise
