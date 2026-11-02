#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
import abc
import concurrent.futures as cf
import os
import threading
from typing import Union

import requests
import json
import os.path as op
import threading

from cattrs import ClassValidationError, transform_error

import mscs_checkpoint_util as cutil
from mscs_checkpointer import KVCheckpointer, FileCheckpointer
import mscs_consts
import mscs_util
import splunk.rest as rest
from splunk_ta_mscs_i2.models import (
    ProxyConfig,
    AzureStorageAccountConfig,
    format_validation_exception,
)


class BlobKeyException(Exception):
    def __init__(self, blob_name=None):
        self.blob_name = blob_name


# if the key can not be stored by kvstore
# ie: invalid key name
class BlobKeyError(BlobKeyException):
    pass


# if another process is run this key.
class BlobKeyBusy(BlobKeyException):
    pass


# Checkpoint doesn't exist - new file or this may happen due to performance issues with KV checkpointer
class BlobKeyNotFound(BlobKeyException):
    pass


# Checkpoint wasn't updated - this may happen due to performance issues with KV checkpointer
class BlobKeyNotUpdated(BlobKeyException):
    pass


class StorageDispatcher(metaclass=abc.ABCMeta):  # type: ignore
    DEFAULT_WORKER_THREADS_NUM = 10

    def __init__(
        self,
        all_conf_contents,
        meta_config,
        task_config,
        data_writer,
        logger,
        use_kv=False,
    ):
        self._all_conf_contents = all_conf_contents
        self._meta_config = meta_config
        self._task_config = task_config
        self._data_writer = data_writer
        self._logger = logger

        self._proxy_config = self.get_proxy_config()
        self._storage_account_config = self.get_storage_account_from_all_confs()
        self._checkpointer = self._create_checkpointer(use_kv)
        self._worker_threads_num = int(
            mscs_util.find_config_in_settings(
                mscs_consts.WORKER_THREADS_NUM,
                self.DEFAULT_WORKER_THREADS_NUM,
                self._task_config,
                self._all_conf_contents[mscs_consts.GLOBAL_SETTINGS][
                    mscs_consts.PERFORMANCE_TUNING_SETTINGS
                ],
            )
        )
        self._executor = None
        self._storage_dispatcher = threading.Thread(
            target=self._dispatch_storage_list, daemon=True
        )
        self._canceled = threading.Event()
        self._sub_canceled_lst = []

    def start(self):
        self._logger.info("worker_threads_num=%s", self._worker_threads_num)
        self._executor = cf.ThreadPoolExecutor(max_workers=self._worker_threads_num)
        self._storage_dispatcher.start()

    def cancel(self):
        self._canceled.set()

    def is_alive(self):
        return self._storage_dispatcher.is_alive()  # pylint: disable=no-member

    def get_checkpointer(self):
        return self._checkpointer

    @abc.abstractmethod
    def _get_patterns(self):
        pass

    @abc.abstractmethod
    def _get_ckpt(self, storage_info):
        pass

    @abc.abstractmethod
    def _get_sub_task_config(self, storage_info, ckpt):
        pass

    @abc.abstractmethod
    def _get_running_task(self):
        pass

    @abc.abstractmethod
    def _dispatch_tasks(self, patterns):
        pass

    def _dispatch_storage_list(self):
        try:
            self._logger.info("Starting to dispatch storage list")
            self._do_dispatch()
            self._logger.info("Finished dispatching storage list.")

        except BrokenPipeError as e:
            self._logger.error(
                f'message="Broken pipe error while dispatching storage list: {e}"',
                exc_info=e,
            )
            for sub_canceled in self._sub_canceled_lst:
                sub_canceled.set()

            if self._executor:
                self._executor.shutdown(wait=False)
        except Exception as e:
            self._logger.exception(
                f'message="Unknown exception while dispatching storage list: {e}"',
                exc_info=e,
            )
            for sub_canceled in self._sub_canceled_lst:
                sub_canceled.set()
            if self._executor:
                self._executor.shutdown(wait=False)

    def _do_dispatch(self):
        patterns = self._get_patterns()
        self._dispatch_tasks(patterns)

    def _create_checkpointer(
        self, use_kv: bool = False
    ) -> Union[KVCheckpointer, FileCheckpointer]:
        """
        creates Checkpointer
        this checkpointer should be the only one for blobstore

        if _get_checkpoint_dir returns an exisiting directory then
        we create the FileCheckpointer class for migration
        """

        if not use_kv:
            checkpoint_dir = self.get_checkpoint_dir()
            return FileCheckpointer(checkpoint_dir)

        input_id = self.get_checkpoint_input_id()
        return KVCheckpointer(
            self._meta_config,
            input_id=input_id,
        )

    def get_checkpoint_dir(self):
        account_name = self.get_storage_account_name()
        qualified_dir_name = cutil.get_checkpoint_name(
            (self._task_config[mscs_consts.STANZA_NAME], account_name)
        )
        return os.path.join(
            self._meta_config[mscs_consts.CHECKPOINT_DIR], qualified_dir_name
        )

    def get_checkpoint_input_id(self):
        account_name = self.get_storage_account_name()
        return cutil.get_checkpoint_name(
            (
                self._task_config.get(mscs_consts.CONTAINER_NAME, "-"),
                account_name,
                self._get_index_name(),
            )
        )

    def get_storage_account_name(self):
        account_stanza_name = self._task_config.get(mscs_consts.ACCOUNT, "Unknown")
        try:
            account_info = self._all_conf_contents[mscs_consts.ACCOUNTS][
                account_stanza_name
            ]
            account_name = account_info.get(mscs_consts.ACCOUNT_NAME)
        except KeyError:
            self._logger.error(
                f"message=\"Storage account '{account_stanza_name}' not found in configuration.\""
            )
            account_name = account_stanza_name
        return account_name

    def _get_index_name(self):
        """Used to get the name of the Index.

        Returns:
            str: Name of the index.
        """
        index = self._task_config.get(mscs_consts.INDEX, "default")
        try:
            if index == "default":
                server_response, server_content = rest.simpleRequest(
                    "/services/data/indexes",
                    sessionKey=self._meta_config["session_key"],
                    method="GET",
                    getargs={"output_mode": "json", "count": 1},
                    raiseAllErrors=True,
                )

                if server_response.status == 200:
                    response_json = json.loads(server_content)
                    index = response_json["entry"][0]["content"]["defaultDatabase"]
        except Exception as e:
            self._logger.error(
                'message="Could not find default index. Using `default`"',
                exc_info=e,
            )
        return index

    def _wait_while_full(self, fs, max_threads):
        """
        Waits while threads running are greater than max_threads * 2

        fs:  a list of futures
        max_threads: max number of threads to run

        returns: list of still running futures(threads)

        """
        while len(fs) > (max_threads * 2):
            if self._canceled.is_set():
                return fs
            res = cf.wait(fs=fs, timeout=10, return_when=cf.FIRST_COMPLETED)
            fs = list(res.not_done)
        return fs

    def _wait_fs(self, fs):
        while True:
            res = cf.wait(fs=fs, timeout=10, return_when=cf.ALL_COMPLETED)
            if not res.not_done:
                break
            if self._canceled.is_set():
                break

    def _cancel_sub_tasks(self, canceled, sub_canceled_list=[]):
        if canceled.is_set():
            for sub_canceled in sub_canceled_list:
                sub_canceled.set()
            self._executor.shutdown(wait=False)
            return True
        return False

    def get_proxy_config(self):
        global_settings = self._all_conf_contents.get(mscs_consts.GLOBAL_SETTINGS, {})
        proxy_settings = global_settings.get(mscs_consts.PROXY, {})
        proxy_config = ProxyConfig.from_dict(proxy_settings)

        if not proxy_config.proxy_dict:
            self._logger.info("Proxy is disabled.")
        else:
            self._logger.info("Proxy is enabled.")
        return proxy_config

    def get_storage_account_from_all_confs(self) -> AzureStorageAccountConfig:
        try:
            storage_account_name = self._task_config[mscs_consts.ACCOUNT]
            storage_account_info = self._all_conf_contents[mscs_consts.ACCOUNTS][
                storage_account_name
            ]
            return AzureStorageAccountConfig.from_dict(storage_account_info)
        except KeyError as e:
            self._logger.error('message="Failed to read config files"', exc_info=e)
            raise e
        except ClassValidationError as e:
            self._logger.error(
                f'message="Failed to validate Azure Storage Account model for the account: {storage_account_name}. '
                f'Error details: {transform_error(e, format_exception=format_validation_exception)}"',
                exc_info=e,
            )
            raise e
