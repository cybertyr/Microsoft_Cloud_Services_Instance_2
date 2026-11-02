#!/usr/bin/python
#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
import abc
import logging

from azure.eventhub.extensions import checkpointstoreblob

import mscs_consts
from splunk_ta_mscs.models import (
    AzureStorageAccountConfig,
    ProxyConfig,
)

from azure.data.tables import (  # isort: skip # pylint: disable=import-error
    TableServiceClient,
)
from azure.storage.blob import (  # isort: skip # pylint: disable=import-error
    BlobServiceClient,
    ContainerClient,
    BlobClient,
)


class BlobModeType:
    APPEND = "append"
    RANDOM = "random"


class BlobType:
    BLOCK_BLOB = "BlockBlob"
    APPEND_BLOB = "AppendBlob"
    PAGE_BLOB = "PageBlob"


def _create_table_service(
    storage_account: AzureStorageAccountConfig, proxies: dict = None
):
    return TableServiceClient(
        endpoint=storage_account.class_type.base_table_account_url.format(
            account_name=storage_account.name
        ),
        credential=storage_account.secret_type.get_table_credentials(storage_account),
        proxies=proxies,
    )


def _create_blob_service(
    storage_account: AzureStorageAccountConfig,
    proxies: dict = None,
    read_timeout: int = 60,
):
    return BlobServiceClient(
        account_url=storage_account.class_type.base_blob_account_url.format(
            account_name=storage_account.name
        ),
        credential=storage_account.secret_type.get_blob_credentials(storage_account),
        proxies=proxies,
        read_timeout=read_timeout,
    )


def _create_blob_checkpoint_store_service(
    storage_account: AzureStorageAccountConfig,
    container_name: str,
    proxies: dict = None,
) -> checkpointstoreblob.BlobCheckpointStore:

    return checkpointstoreblob.BlobCheckpointStore(
        blob_account_url=storage_account.class_type.base_blob_account_url.format(
            account_name=storage_account.name
        ),
        container_name=container_name,
        credential=storage_account.secret_type.get_blob_checkpoint_credentials(
            storage_account
        ),
        proxies=proxies,
    )


class StorageService(metaclass=abc.ABCMeta):  # type: ignore
    def __init__(
        self,
        all_conf_contents: dict,
        meta_config: dict,
        task_config: dict,
        proxy_config: ProxyConfig,
        storage_account_config: AzureStorageAccountConfig,
        logger: logging.Logger,
    ):
        self._all_conf_contents = all_conf_contents
        self._meta_config = meta_config
        self._task_config = task_config
        self._proxy_config = proxy_config
        self._logger = logger
        self._storage_account_config = storage_account_config
        self._storage_service = None

    def get_service(self):
        if not self._storage_service:
            self._storage_service = self._create_service()
        return self._storage_service

    @abc.abstractmethod
    def _create_service(self):
        pass


class TableStorageService(StorageService):
    def _create_service(self):
        sv = _create_table_service(
            storage_account=self._storage_account_config,
            proxies=self._proxy_config.proxy_dict,
        )
        return sv

    def get_table_client(self, table_name: str):
        table_service = self.get_service()
        table_client = table_service.get_table_client(table_name)
        return table_client


class BlobStorageService(StorageService):
    def __init__(
        self,
        all_conf_contents: dict,
        meta_config: dict,
        task_config: dict,
        proxy_config: ProxyConfig,
        storage_account_config: AzureStorageAccountConfig,
        logger: logging.Logger = None,
    ):
        super().__init__(
            all_conf_contents,
            meta_config,
            task_config,
            proxy_config,
            storage_account_config,
            logger,
        )
        self._container_name = self._task_config[mscs_consts.CONTAINER_NAME]
        self._container_client = None
        self.read_timeout = self._task_config[mscs_consts.READ_TIMEOUT]

    def _create_service(self):
        return _create_blob_service(
            storage_account=self._storage_account_config,
            proxies=self._proxy_config.proxy_dict,
            read_timeout=int(self.read_timeout),
        )

    def get_container_client(self) -> ContainerClient:
        if not self._container_client:
            blob_service = self.get_service()
            self._container_client = blob_service.get_container_client(
                self._container_name
            )
        return self._container_client

    def get_blob_client(self, blob_name: str) -> BlobClient:
        container_client = self.get_container_client()
        blob_client = container_client.get_blob_client(blob_name)
        return blob_client
