#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
import queue

import mscs_consts
import mscs_data_writer as mdw
import mscs_storage_table_dispatcher as mstd
import mscs_logger as logger


class StorageTableListDataCollector:
    TIMEOUT = 3

    def __init__(self, all_conf_contents, meta_config, task_config):
        self._all_conf_contents = all_conf_contents
        self._meta_config = meta_config
        self._task_config = task_config
        self._data_writer = mdw.DataWriter()

        log_prefix = self._get_logger_prefix()
        self._logger = logger.logger_for(log_prefix)

        self._logger.debug(f"{task_config['stanza_name']} settings: {task_config}")

        self._storage_dispatcher = mstd.StorageTableDispatcher(
            all_conf_contents, meta_config, task_config, self._data_writer, self._logger
        )
        self._checkpointer = self._storage_dispatcher.get_checkpointer()

    def collect_data(self):
        try:
            self._logger.info("Starting to collect data.")

            self._storage_dispatcher.start()

            self._logger.info("Starting to get data from data_writer.")
            need_get_data = False
            # When we received the stop signal or the table_dispatcher thread is terminated,
            # we will break the loop.
            while True:
                try:
                    events, key, ckpt = self._data_writer.get_data(timeout=self.TIMEOUT)
                    if key:
                        self._checkpointer.update(key, ckpt)
                    stop = yield events, None
                    if stop:
                        self._storage_dispatcher.cancel()
                        break

                    if not self._storage_dispatcher.is_alive():
                        need_get_data = True
                        break
                except queue.Empty:
                    if not self._storage_dispatcher.is_alive():
                        need_get_data = True
                        break
                    else:
                        continue

            if not need_get_data:
                self._checkpointer.close()
                return

            self._logger.info("Retrieve the remain data from data_writer.")

            while True:
                try:
                    events, key, ckpt = self._data_writer.get_data(block=False)
                    if key:
                        self._checkpointer.update(key, ckpt)
                    yield events, None
                except queue.Empty:
                    break

            self._checkpointer.close()
        except Exception as e:
            self._logger.exception(
                f'message="Exception@collect_data(): {e}"', exc_info=e
            )
            try:
                self._checkpointer.close()
            except Exception as e:
                self._logger.exception(
                    f'message="Failed to close checkpointer: {e}"', exc_info=e
                )
                self._storage_dispatcher.cancel()
            self._storage_dispatcher.cancel()

    def _init_from_task_config(self):
        self._table_list = self._task_config.get(mscs_consts.TABLE_LIST)

    def _get_logger_prefix(self):
        account_stanza_name = self._task_config[mscs_consts.ACCOUNT]
        account_info = self._all_conf_contents[mscs_consts.ACCOUNTS][
            account_stanza_name
        ]
        account_name = account_info.get(mscs_consts.ACCOUNT_NAME)
        pairs = [
            '{}="{}"'.format(
                mscs_consts.STANZA_NAME, self._task_config[mscs_consts.STANZA_NAME]
            ),
            '{}="{}"'.format(mscs_consts.ACCOUNT_NAME, account_name),
            '{}="{}"'.format(
                mscs_consts.TABLE_LIST, self._task_config[mscs_consts.TABLE_LIST]
            ),
        ]
        return "[{}]".format(" ".join(pairs))
