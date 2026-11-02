#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
import copy
import re
import threading

import mscs_consts
import mscs_storage_dispatcher as msd
import mscs_storage_service as mss
import mscs_storage_table_data_collector as mstdc
from mscs_storage_dispatcher import BlobKeyError


class StorageTableDispatcher(msd.StorageDispatcher):
    def __init__(
        self, all_conf_contents, meta_config, task_config, data_writer, logger
    ):
        super(StorageTableDispatcher, self).__init__(
            all_conf_contents, meta_config, task_config, data_writer, logger
        )
        self._storage_service = mss.TableStorageService(
            all_conf_contents=all_conf_contents,
            meta_config=meta_config,
            task_config=task_config,
            proxy_config=self._proxy_config,
            storage_account_config=self._storage_account_config,
            logger=logger,
        )

        patterns, metrics_tables = self._parse_table_filter_string(
            filter_string=self._task_config.get(mscs_consts.TABLE_LIST, "")
        )
        self._cached_patterns = patterns
        self._metrics_tables = metrics_tables

    def _compile_patterns(self, pattern_strings):
        compiled, names = [], []
        for ps in pattern_strings:
            try:
                compiled.append(self._compile_table_name(ps))
                names.append(ps)
            except re.error as e:
                self._logger.warning(
                    f'message="The table pattern={ps} is invalid. error={e}"',
                    exc_info=e,
                )
        self._logger.info("The valid patterns=[%s]", ",".join(names))
        return compiled

    def _parse_table_filter_string(self, filter_string):
        self._logger.info("The table filter patterns=[%s]", filter_string)

        if not filter_string:
            return [], []
        pattern_names, metrics_tables = [], []

        for rp in filter_string.split(","):
            text = rp.strip()
            if not text:
                continue
            if text.startswith("$"):
                metrics_tables.append(text)
            else:
                pattern_names.append(text)
        self._logger.info("The metrics table names=%s", metrics_tables)
        return self._compile_patterns(pattern_names), metrics_tables

    def _get_patterns(self):
        return self._cached_patterns

    @staticmethod
    def _compile_table_name(table_name):
        if table_name.startswith(":"):
            table_name = table_name[1:] + "$"
        else:
            table_name = table_name.replace("*", ".*") + "$"
        return re.compile(table_name, re.IGNORECASE)

    def _dispatch_tasks(self, patterns):
        # storage_name_set = set()
        storage_info_lst = self._get_storage_info_list(patterns)
        self._logger.info(
            "The number of qualified_storage is %s", len(storage_info_lst)
        )

        task_futures = []
        self._sub_canceled_lst = []
        for storage_info in storage_info_lst:
            try:
                ckpt = self._get_ckpt(storage_info)
            except BlobKeyError as e:
                self._logger.warning(
                    f'message="Unsupported blob name, it contains some none ASCII characters" '
                    f"blob={e.blob_name}",
                    exc_info=e,
                )
                continue
            sub_task_config = self._get_sub_task_config(storage_info, ckpt)
            if not sub_task_config:
                continue
            sub_canceled = threading.Event()
            running_task = self._get_running_task()
            task_future = self._executor.submit(
                running_task,
                self._all_conf_contents,
                self._meta_config,
                sub_task_config,
                ckpt,
                sub_canceled,
                self._data_writer,
                self._logger,
                self._proxy_config,
                self._storage_account_config,
            )
            task_futures.append(task_future)
            self._sub_canceled_lst.append(sub_canceled)
        self._wait_fs(task_futures)
        if self._cancel_sub_tasks(self._canceled, self._sub_canceled_lst):
            return
        self._executor.shutdown()

    def _get_storage_info_list(self, patterns):
        table_service = self._storage_service.get_service()
        tables = table_service.list_tables()

        table_list = [table.name for table in tables]
        self._logger.info("The table list=[%s]", ",".join(table_list))

        qualified_tables = [
            name for name in table_list if self._is_match(name, patterns)
        ]

        if self._metrics_tables:
            # Metrics tables will only be processed once
            qualified_tables.extend(self._metrics_tables)
            self._metrics_tables = None

        self._logger.info("The qualified table list=[%s]", ",".join(qualified_tables))

        return qualified_tables

    def _get_ckpt(self, storage_info):
        return self._checkpointer.get(storage_info)

    def _get_sub_task_config(self, storage_info, ckpt):
        sub_task_config = copy.copy(self._task_config)
        sub_task_config[mscs_consts.TABLE_NAME] = storage_info
        return sub_task_config

    def _get_running_task(self):
        return mstdc.running_task

    @staticmethod
    def _is_match(storage_name, patterns):
        return any(pattern for pattern in patterns if pattern.match(storage_name))
