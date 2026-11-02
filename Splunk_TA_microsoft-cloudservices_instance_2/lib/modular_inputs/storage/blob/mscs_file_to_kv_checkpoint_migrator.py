#!/usr/bin/python
# #
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#

import logging
import os
import dataclasses
import shutil
import threading
import time
from typing import Generator

from azure.storage.blob import ContainerClient
from solnlib import utils

import mscs_consts
import mscs_checkpoint_util
from splunk import rest
from mscs_checkpointer import FileCheckpointer, KVCheckpointer
from mscs_util import get_conf_file_info, IntervalTimer


class MigrationStoppedError(Exception):
    pass


class MigrationInProgressError(Exception):
    pass


class MigrationAlreadyCompletedError(Exception):
    pass


class CheckpointDirectoryEmptyError(Exception):
    pass


class BatchQueue:
    def __init__(self, batch_size: int):
        self._batch_size = batch_size
        self._queue = {}

    def add(self, ckpt_name: str, record: dict):
        self._queue[ckpt_name] = record

    def is_overflow(self) -> bool:
        return len(self._queue) >= self._batch_size

    def flush(self):
        if not self._queue:
            return
        self._queue.clear()

    def is_empty(self):
        return not bool(self._queue)

    def get_queue_length(self):
        return len(self._queue)

    def get_list_of_ckpts(self):
        return list(self._queue.values())

    def get_batch_size(self):
        return self._batch_size


@dataclasses.dataclass
class FileToKvCheckpointMigrationStats:
    blobs_in_container: int = 0
    blobs_filtered: int = 0
    ckpts_in_ckpt_dir: int = 0
    blobs_scanned: int = 0
    ckpts_in_batch_queue: int = 0
    ckpts_migrated: int = 0
    files_deleted: int = 0
    file_checkpointer_get: int = 0


class FileToKvCheckpointMigrator:
    LOCK_TIME = 300
    MIGRATION_STATUS_INTERVAL = 10

    def __init__(
        self,
        kv_checkpointer: KVCheckpointer,
        file_checkpointer: FileCheckpointer,
        container_client: ContainerClient,
        batch_size: int,
        session_key: str,
        server_uri: str,
        modinput_name: str,
        is_migrated_in_task_config: str,
        logger: logging.Logger,
        stop_event: threading.Event,
    ):
        self._kv_checkpointer = kv_checkpointer
        self._file_checkpointer = file_checkpointer
        self._ckpt_dir = self._file_checkpointer.get_checkpoint_dir()
        self._container_client = container_client
        self._session_key = session_key
        self._server_uri = server_uri
        self._modinput_name = modinput_name
        self._is_migrated_in_task_config = is_migrated_in_task_config
        self._logger = logger
        self._stop_event = stop_event
        self._stats = FileToKvCheckpointMigrationStats()
        self._last_processed_blob = None
        self._migration_controller = IntervalTimer(
            interval=self.MIGRATION_STATUS_INTERVAL, function=self._control_migration
        )
        self._batch_ckpt_queue = BatchQueue(batch_size=batch_size)

    def _control_migration(self):
        self._logger.info(f"ckpt_migration_stats={self._stats}")
        self._lock_collection_for_migration()

    def _lock_collection_for_migration(self):
        self._logger.debug(
            f"Locking collection for {self.LOCK_TIME} seconds to migrate checkpoints"
        )
        self._kv_checkpointer.update(
            mscs_consts.FILE_TO_KV_MIGRATION_LOCK, time.time() + self.LOCK_TIME
        )

    def migrate(self) -> bool:
        try:
            migration_generator = self._do_migrate()
            while not self._stop_event.is_set():
                next(migration_generator)
            else:
                raise MigrationStoppedError()
        except StopIteration:
            return True
        except (MigrationAlreadyCompletedError, CheckpointDirectoryEmptyError) as e:
            self._logger.debug(f"Migration not needed. Details: {e}")
            self._kv_checkpointer.update(mscs_consts.FILE_TO_KV_MIGRATION_LOCK, 0)
            self._kv_checkpointer.update(mscs_consts.FILE_TO_KV_MIGRATED, "1")
            return True
        except Exception as e:
            self._logger.error(
                f'message="An error occurred while migrating file checkpoints to KV Store"',
                exc_info=e,
            )
            self._stop_migration()
            raise e

    def _do_migrate(self):
        if self._is_another_migration_process_running():
            raise MigrationInProgressError()
        self._lock_collection_for_migration()
        amount_of_files_in_ckpt_dir = len(os.listdir(self._ckpt_dir))
        if self._is_kv_store_migrated():
            if amount_of_files_in_ckpt_dir:
                self._logger.info(
                    f"Migration was marked as completed, but amount_of_files_in_ckpt_dir={amount_of_files_in_ckpt_dir} is not zero. Removing the remaining files..."
                )
                self._finish_migration_process()
                return True
            else:
                raise MigrationAlreadyCompletedError(
                    "Migration was already completed in previous run."
                )
        if not amount_of_files_in_ckpt_dir:
            raise CheckpointDirectoryEmptyError("Checkpoint directory is empty.")
        self._logger.info(
            f"Migration of checkpoints from File Checkpointer to KV Store started for modinput_name={self._modinput_name}, batch_size={self._batch_ckpt_queue.get_batch_size()}"
        )
        self._send_migration_notification_to_ui(
            f"Migration of checkpoints from File Checkpointer to KV Store started for {self._modinput_name} modinput."
        )
        self._migration_controller.start()
        self._stats.ckpts_in_ckpt_dir = amount_of_files_in_ckpt_dir
        blob_names_gen = self._get_blob_names()
        for blob_name in blob_names_gen:
            self._migrate_single_checkpoint(blob_name)
            yield
        self._batch_ckpt_update()
        self._finish_migration_process()
        self._send_migration_notification_to_ui(
            f"File checkpoints for {self._modinput_name} modinput are now migrated to KV Store."
        )
        self._logger.info(
            f"File checkpoints for modinput_name={self._modinput_name} are now migrated to KV Store."
        )

    def _is_another_migration_process_running(self) -> bool:
        ckpt_migration_lock = self._kv_checkpointer.get(
            mscs_consts.FILE_TO_KV_MIGRATION_LOCK
        )
        if ckpt_migration_lock and ckpt_migration_lock > time.time():
            self._logger.debug(
                "Checkpoint migration is currently running in another modinput. This modinput process is going to stop..."
            )
            return True
        return False

    def _finish_migration_process(self):
        self._delete_ckpt_files_and_dir()
        self._kv_checkpointer.update(mscs_consts.FILE_TO_KV_MIGRATED, "1")
        self._stop_migration()

    def _get_blob_names(self):
        blob_names_gen = self._container_client.list_blob_names()
        self._stats.blobs_in_container = len(list(blob_names_gen))
        blob_names_gen = self._container_client.list_blob_names()
        blob_names_gen = self._filter_migrated_blobs(blob_names_gen)
        return blob_names_gen

    def _filter_migrated_blobs(self, blob_names_gen: Generator) -> Generator:
        filtered_blobs = 0
        last_migrated_blob = self._kv_checkpointer.get(
            mscs_consts.FILE_TO_KV_MIGRATION_LAST_MIGRATED_BLOB
        )
        if not last_migrated_blob:
            return blob_names_gen

        for blob_name in blob_names_gen:
            filtered_blobs += 1
            if blob_name == last_migrated_blob:
                self._logger.info(
                    f"Resuming migration process from blob_name={blob_name}. Skipped filtered_blobs={filtered_blobs} blobs as they were already migrated. "
                )
                self._send_migration_notification_to_ui(
                    f"Resuming migration process from {blob_name} blob for {self._modinput_name} modinput. Skipped {filtered_blobs} blobs as they were already migrated. "
                )
                self._stats.blobs_filtered = filtered_blobs
                return blob_names_gen
        else:
            return self._container_client.list_blob_names()

    def _migrate_single_checkpoint(self, blob_name: str):
        self._stats.blobs_scanned += 1
        self._last_processed_blob = blob_name
        ckpt_name = mscs_checkpoint_util.get_blob_checkpoint_name(
            container_name=self._container_client.container_name,
            blob_name=blob_name,
        )
        file_ckpt = self._file_checkpointer.get(key=ckpt_name)
        self._stats.file_checkpointer_get += 1
        if not file_ckpt:
            self._logger.debug(
                f"File checkpoint not available for blob_name={blob_name}"
            )
            return
        self._batch_ckpt_queue.add(
            ckpt_name, self._kv_checkpointer.get_formatted_record(ckpt_name, file_ckpt)
        )
        self._stats.ckpts_in_batch_queue += 1
        self._logger.debug(f"blob_name={blob_name} added to batch migration queue")
        if self._batch_ckpt_queue.is_overflow():
            self._batch_ckpt_update()

    def _batch_ckpt_update(self):
        if self._batch_ckpt_queue.is_empty():
            return
        batch_update_size = self._batch_ckpt_queue.get_queue_length()
        self._logger.debug(
            f"Performing batch checkpoint update for batch_update_size={batch_update_size} blobs"
        )
        self._batch_ckpt_queue.add(
            mscs_consts.FILE_TO_KV_MIGRATION_LAST_MIGRATED_BLOB,
            self._kv_checkpointer.get_formatted_record(
                mscs_consts.FILE_TO_KV_MIGRATION_LAST_MIGRATED_BLOB,
                self._last_processed_blob,
            ),
        )
        self._kv_checkpointer.batch_save(self._batch_ckpt_queue.get_list_of_ckpts())
        self._stats.ckpts_migrated += batch_update_size
        self._batch_ckpt_queue.flush()
        self._stats.ckpts_in_batch_queue = 0

    def _release_migration_lock(self):
        self._logger.debug("Releasing migration lock")
        self._kv_checkpointer.update(mscs_consts.FILE_TO_KV_MIGRATION_LOCK, 0)

    def _delete_ckpt_files_and_dir(self) -> None:
        try:
            amount_of_ckpts_in_dir = len(os.listdir(self._ckpt_dir))
            self._logger.info(
                f"Removing checkpoint directory containing amount_of_ckpts_in_dir={amount_of_ckpts_in_dir} files."
            )
            for file in os.scandir(self._ckpt_dir):
                file_path = f"{self._ckpt_dir}/{file.name}"
                try:
                    os.remove(file_path)
                except FileNotFoundError:
                    self._logger.debug(
                        f"File not found for ckpt_not_found_path={file_path}. Continue deleting remaining files"
                    )
                self._stats.files_deleted += 1
            self._logger.info("Checkpoint files deleted successfully.")
            shutil.rmtree(self._ckpt_dir)
            self._logger.info(
                f"Checkpoint directory ckpt_dir={self._ckpt_dir} deleted successfully."
            )
        except OSError as e:
            self._logger.error(
                f'message=f"An error occurred while deleting checkpoint directory: ckpt_dir={self._ckpt_dir}"',
                exc_info=e,
            )
            raise e

    def _send_migration_notification_to_ui(self, message: str) -> None:
        url = f"{self._server_uri}/services/messages"
        payload = {
            "name": f"File to KV checkpoint migration status for {self._modinput_name} modinput, time: {time.time()}.",
            "value": f"Splunk Add-on for Microsoft Cloud Services (Instance 2): {message}",
            "severity": "info",
        }
        server_response, server_content = rest.simpleRequest(
            url,
            sessionKey=self._session_key,
            method="POST",
            postargs=payload,
            raiseAllErrors=True,
        )
        if server_response.status != 201:
            self._logger.warning(
                f'message="Failed to send UI notification for input modinput_name={self._modinput_name}"'
            )

    def _stop_migration(self):
        self._logger.info("Stopping the process of checkpoint migration.")
        self._migration_controller.stop()
        self._logger.info("Releasing migration lock")
        self._kv_checkpointer.update(mscs_consts.FILE_TO_KV_MIGRATION_LOCK, 0)
        self._logger.info(f"ckpt_migration_stats={self._stats}")

    def _is_kv_store_migrated(self) -> bool:
        """
        In version 5.5.0 checkpoint migration status is being stored in KV.
        In version 5.0.0 - 5.4.X it was stored in inputs.conf
        That's why we need to check for migration status in few places
        """
        if utils.is_true(self._kv_checkpointer.get(mscs_consts.FILE_TO_KV_MIGRATED)):
            return True
        if utils.is_true(self._is_migrated_in_task_config):
            return True
        inputs_conf = get_conf_file_info(
            session_key=self._session_key,
            conf_file_name="inputs",
            only_current_app=True,
        )
        stanza_name = f"mscs_i2_storage_blob://{self._modinput_name}"
        return utils.is_true(
            inputs_conf.get(stanza_name, {}).get(mscs_consts.IS_MIGRATED)
        )
