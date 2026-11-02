#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
import threading

import splunktaucclib.data_collection.ta_data_client as dc

import mscs_logger as logger
import mscs_storage_blob_list_data_collector as msbldc
from mscs_util import setup_log_level


class StorageBlobDataClient(dc.TaDataClient):
    def __init__(self, all_conf_contents, meta_config, task_config, ckpt, chp_mgr):
        super(StorageBlobDataClient, self).__init__(
            all_conf_contents, meta_config, task_config, ckpt, chp_mgr
        )
        log_level = task_config.get("agent")
        setup_log_level(log_level)
        self._logger = logger.logger_for("StorageBlobDataClient")

        self._execute_times = 0
        self._stop_event = threading.Event()
        self._gen = self.get_contents()

    def stop(self):
        """
        handle stop control command
        """

        self._stop_event.set()
        # normaly base class just set self._stop as True
        super(StorageBlobDataClient, self).stop()

    def get(self):
        """
        get events
        """
        self._execute_times += 1
        if self.is_stopped():
            self._logger.info(
                'message="Stop signal received. '
                'Propagating stop to data collector and exiting gracefully."'
            )
            self._gen.send(self.is_stopped())
            raise StopIteration
        try:
            if self._execute_times == 1:
                return next(self._gen)
            return self._gen.send(self.is_stopped())
        except BrokenPipeError:
            self._logger.error(
                'message="Output stream closed (BrokenPipeError). '
                "The splunkd process may have terminated the pipe. "
                'Signaling data collector to stop."'
            )
            self._stop_event.set()
            # BrokenPipeError is converted to StopIteration because StopIteration
            # is the contract of get() — the TADataCollector framework uses it to
            # exit the collection loop cleanly. Re-raising BrokenPipeError would be
            # unhandled by the caller and cause an unexpected crash.
            raise StopIteration

    def get_contents(self):
        data_collector = msbldc.StorageBlobListDataCollector(
            all_conf_contents=self._all_conf_contents,
            meta_config=self._meta_config,
            task_config=self._task_config,
            stop_event=self._stop_event,
        )
        return data_collector.collect_data()
