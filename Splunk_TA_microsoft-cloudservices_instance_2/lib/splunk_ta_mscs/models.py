#!/usr/bin/python
#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
from __future__ import annotations

from abc import ABC
from typing import Optional, Union, Type
from uuid import UUID

from attr import field, validators
from attrs import converters, define
from azure.core.credentials import AzureNamedKeyCredential, AzureSasCredential
from azure.identity import KnownAuthorities
from cattrs import Converter
from cattrs.gen import make_dict_structure_fn, override
from cattrs.v import format_exception
from urllib.parse import quote as urlquote

from solnlib import utils

import mscs_consts


class ModelValidationException(Exception):
    pass


def format_validation_exception(
    exc: BaseException, attr_type: Union[Type, None]
) -> str:
    if isinstance(exc, ModelValidationException):
        return str(exc)
    return format_exception(exc, attr_type)


def urlqoute(value: str) -> str:
    if value:
        return urlquote(value, encoding="utf-8", safe="")
    return None


def strict_str(v) -> str:
    if v is None:
        raise ValueError("Unable to construct str from None")
    return str(v)


def get_converter() -> Converter:
    converter = Converter()
    converter.register_structure_hook(UUID, lambda v, _: UUID(v))
    converter.register_structure_hook(
        AzureCloud,
        lambda v, _: CONFIG_AZURE_ACCOUNT_CLASS_TYPES.get(v, AzurePublicCloud()),
    )
    converter.register_structure_hook(bool, lambda v, _: utils.is_true(v))
    converter.register_structure_hook(str, lambda v, _: strict_str(v))
    converter.register_structure_hook(
        ProxyConfig,
        make_dict_structure_fn(ProxyConfig, converter, proxy_dict=override(omit=True)),
    )
    converter.register_structure_hook(
        AccountSecretType,
        lambda v, _: CONFIG_AZURE_STORAGE_ACCOUNT_SECRET_TYPE[v],
    )

    return converter


@define
class ProxyConfig:
    enabled: bool = field(converter=utils.is_true)
    type: Optional[str] = field(
        validator=validators.optional(validators.matches_re("https?")), default=None
    )
    host: Optional[str] = field(
        validator=validators.optional(
            [
                validators.instance_of(str),
                validators.min_len(0),
                validators.max_len(4096),
                validators.matches_re(mscs_consts.PROXY_HOST_PATTERN),
            ]
        ),
        default=None,
    )
    port: Optional[int] = field(
        converter=converters.optional(int),
        validator=validators.optional([validators.ge(1), validators.le(65535)]),
        default=None,
    )
    username: Optional[str] = field(
        validator=validators.optional([validators.min_len(0), validators.max_len(50)]),
        default=None,
    )
    password: Optional[str] = field(default=None)
    url: Optional[str] = field(init=False, default=None)
    proxy_dict: dict = field(init=False)

    def __attrs_post_init__(self):
        self.set_url()
        self.set_proxy_dict()

    def set_url(self):
        if not self.enabled:
            self.url = None
        elif not self.type or not self.host or not self.port:
            raise ValueError("Proxy config values missing")
        else:
            authority = f"{self.host}:{self.port}"

            if self.username and self.password:
                authority = (
                    f"{urlqoute(self.username)}:{urlqoute(self.password)}@{authority}"
                )
            elif self.username:
                authority = f"{urlqoute(self.username)}@{authority}"

            self.url = f"{self.type}://{authority}"

    def set_proxy_dict(self):
        if not self.enabled:
            self.proxy_dict = {}
        else:
            self.proxy_dict = {"http": self.url, "https": self.url}

    @username.validator
    def check_username_exists_when_password_exists(self, attribute, value):
        if self.password and not self.username:
            raise ValueError(
                "Username is required if password is specified",
            )

    @classmethod
    def from_dict(cls, config) -> ProxyConfig:
        if not isinstance(config, dict):
            raise ValueError("Proxy config is not a dict")

        proxy = {
            "enabled": config.get(mscs_consts.PROXY_ENABLED),
            "type": config.get(mscs_consts.PROXY_TYPE),
            "host": config.get(mscs_consts.PROXY_URL),
            "port": config.get(mscs_consts.PROXY_PORT),
            "username": config.get(mscs_consts.PROXY_USERNAME),
            "password": config.get(mscs_consts.PROXY_PASSWORD),
        }

        return get_converter().structure(proxy, cls)


@define
class AccountSecretType(ABC):
    @staticmethod
    def strip(secret: str):
        return secret

    @staticmethod
    def get_blob_credentials(storage_account: AzureStorageAccountConfig):
        raise NotImplementedError()

    @staticmethod
    def get_table_credentials(storage_account: AzureStorageAccountConfig):
        raise NotImplementedError()

    @staticmethod
    def get_blob_checkpoint_credentials(storage_account: AzureStorageAccountConfig):
        raise NotImplementedError()


@define
class NoneSecretType(AccountSecretType):
    @staticmethod
    def get_blob_credentials(storage_account: AzureStorageAccountConfig):
        return None

    @staticmethod
    def get_table_credentials(storage_account: AzureStorageAccountConfig):
        raise Exception("NoneSecret is not supported by storage table service")

    @staticmethod
    def get_blob_checkpoint_credentials(storage_account: AzureStorageAccountConfig):
        raise Exception(
            "NoneSecret is not supported by storage blob checkpoint service"
        )


@define
class AccessKeySecretType(AccountSecretType):
    @staticmethod
    def get_blob_credentials(storage_account: AzureStorageAccountConfig):
        return AzureNamedKeyCredential(storage_account.name, storage_account.secret)

    @staticmethod
    def get_table_credentials(storage_account: AzureStorageAccountConfig):
        return AzureNamedKeyCredential(storage_account.name, storage_account.secret)

    @staticmethod
    def get_blob_checkpoint_credentials(storage_account: AzureStorageAccountConfig):
        return {
            "account_name": storage_account.name,
            "account_key": storage_account.secret,
        }


@define
class SasTokenSecretType(AccountSecretType):
    @staticmethod
    def strip(secret: str):
        return secret.lstrip("?")

    @staticmethod
    def get_blob_credentials(storage_account: AzureStorageAccountConfig):
        return AzureSasCredential(storage_account.secret)

    @staticmethod
    def get_table_credentials(storage_account: AzureStorageAccountConfig):
        return AzureSasCredential(storage_account.secret)

    @staticmethod
    def get_blob_checkpoint_credentials(storage_account: AzureStorageAccountConfig):
        return storage_account.secret


CONFIG_AZURE_STORAGE_ACCOUNT_SECRET_TYPE = {
    "0": NoneSecretType(),
    "1": AccessKeySecretType(),
    "2": SasTokenSecretType(),
}


class CloudEndpoints:
    def __init__(self, resource_manager: str) -> None:
        self.resource_manager = resource_manager


class Cloud:
    def __init__(self, name: str, endpoints: CloudEndpoints) -> None:
        self.name = name
        self.endpoints = endpoints


@define
class AzureCloud(ABC):
    cloud_environment: Cloud
    authority_url: str
    analytics_endpoint: str
    base_blob_account_url: str
    base_table_account_url: str

    def __attrs_post_init__(self):
        setattr(
            self.cloud_environment.endpoints,
            mscs_consts.LOG_ANALYTICS_ENDPOINT,
            self.analytics_endpoint,
        )


@define
class AzurePublicCloud(AzureCloud):
    cloud_environment: Cloud = Cloud(
        name="AzureCloud",
        endpoints=CloudEndpoints(
            resource_manager=mscs_consts.AZURE_CLOUD_PUBLIC_MANAGEMENT_URL
        ),
    )
    authority_url: str = KnownAuthorities.AZURE_PUBLIC_CLOUD
    analytics_endpoint: str = mscs_consts.AZURE_CLOUD_PUBLIC_LOG_ANALYTICS_URL
    base_blob_account_url: str = mscs_consts.AZURE_CLOUD_PUBLIC_BLOB_URL
    base_table_account_url: str = mscs_consts.AZURE_CLOUD_PUBLIC_TABLE_URL


@define
class AzureUsGovCloud(AzureCloud):
    cloud_environment: Cloud = Cloud(
        name="AzureUSGovernment",
        endpoints=CloudEndpoints(
            resource_manager=mscs_consts.AZURE_CLOUD_GOV_MANAGEMENT_URL
        ),
    )
    authority_url: str = KnownAuthorities.AZURE_GOVERNMENT
    analytics_endpoint: str = mscs_consts.AZURE_CLOUD_GOV_LOG_ANALYTICS_URL
    base_blob_account_url: str = mscs_consts.AZURE_CLOUD_GOV_BLOB_URL
    base_table_account_url: str = mscs_consts.AZURE_CLOUD_GOV_TABLE_URL


CONFIG_AZURE_ACCOUNT_CLASS_TYPES = {"1": AzurePublicCloud(), "2": AzureUsGovCloud()}


@define
class AzureAccountConfig:
    client_id: UUID
    tenant_id: UUID
    auth_type: str = mscs_consts.AUTH_TYPE_CLIENT_SECRET
    client_secret: Optional[str] = None
    certificate_private_key: Optional[str] = None
    certificate_password: Optional[str] = None
    class_type: AzureCloud = AzurePublicCloud()
    disabled: bool = False

    def __attrs_post_init__(self):
        """Validate cross-field invariants for auth_type and its required fields."""
        if self.auth_type == mscs_consts.AUTH_TYPE_CLIENT_SECRET:
            if not self.client_secret:
                raise ModelValidationException(
                    "Client secret is required for Client Secret based Authentication"
                )
        elif self.auth_type == mscs_consts.AUTH_TYPE_CLIENT_CERTIFICATE:
            if not self.certificate_private_key:
                raise ModelValidationException(
                    "Certificate (PEM) is required for Certificate based Authentication"
                )
        else:
            raise ModelValidationException(
                f"Unsupported auth_type '{self.auth_type}'. "
                f"Supported values are: "
                f"'{mscs_consts.AUTH_TYPE_CLIENT_SECRET}', "
                f"'{mscs_consts.AUTH_TYPE_CLIENT_CERTIFICATE}'."
            )

    @classmethod
    def from_dict(cls, config: dict) -> AzureAccountConfig:
        if not isinstance(config, dict):
            raise ValueError("Account config is not a dict")

        azure_account_config = {
            "client_id": config.get(mscs_consts.CLIENT_ID),
            "tenant_id": config.get(mscs_consts.TENANT_ID),
            "auth_type": config.get(
                mscs_consts.AUTH_TYPE, mscs_consts.AUTH_TYPE_CLIENT_SECRET
            ),
            "client_secret": config.get(mscs_consts.CLIENT_SECRET),
            "certificate_private_key": config.get(mscs_consts.CERTIFICATE_PRIVATE_KEY),
            "certificate_password": config.get(mscs_consts.CERTIFICATE_PASSWORD),
            "class_type": config.get(mscs_consts.ACCOUNT_CLASS_TYPE),
            "disabled": config.get(mscs_consts.DISABLED),
        }
        return get_converter().structure(azure_account_config, cls)


@define
class AzureStorageAccountConfig:
    name: str
    secret_type: AccountSecretType
    secret: Optional[str] = field()
    class_type: AzureCloud = AzurePublicCloud()
    disabled: bool = False

    def __attrs_post_init__(self):
        self.secret = self.secret_type.strip(self.secret)

    @secret.validator
    def validate_account_secret(self, attribute, value):
        if not self.secret and self.secret_type != NoneSecretType():
            raise ModelValidationException(
                f"Azure Storage Account validation failed. Account secret is missing"
            )

    @classmethod
    def from_dict(cls, config: dict) -> AzureStorageAccountConfig:
        if not isinstance(config, dict):
            raise ValueError("Azure storage account config is not a dict")

        azure_storage_account = {
            "name": config.get(mscs_consts.ACCOUNT_NAME),
            "secret_type": config.get(mscs_consts.ACCOUNT_SECRET_TYPE),
            "secret": config.get(mscs_consts.ACCOUNT_SECRET),
            "class_type": config.get(mscs_consts.ACCOUNT_CLASS_TYPE),
            "disabled": config.get(mscs_consts.DISABLED),
        }
        return get_converter().structure(azure_storage_account, cls)
