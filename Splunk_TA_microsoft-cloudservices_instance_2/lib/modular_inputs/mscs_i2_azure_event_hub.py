#!/usr/bin/python
#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
from __future__ import absolute_import

import json
import os
import random
import time
import uuid
import datetime
from dataclasses import dataclass
from enum import Enum
from os import path
from threading import Event, Lock, Thread
from typing import Any, Callable, Dict, List, Optional, Tuple

from cattrs import ClassValidationError, transform_error

import import_declare_test  # noqa: F401
from azure.identity import ClientSecretCredential
from splunksdc import logging
from splunksdc.collector import SimpleCollectorV1
from splunksdc.utils import LogExceptions, LogWith

from solnlib.observability import ATTR_MODINPUT_NAME, StanzaObservabilityRecorder

from azure.eventhub import (  # isort: skip
    CheckpointStore,
    EventHubConsumerClient,
    TransportType,
    PartitionContext,
    CloseReason,
    LoadBalancingStrategy,
)  # isort: skip
from azure.eventhub.exceptions import AuthenticationError  # isort: skip
from splunksdc.config import (  # isort: skip
    BooleanField,
    IntegerField,
    StanzaParser,
    StringField,
    LogLevelField,
    ConfigManager,
    Field,
)  # isort: skip

import mscs_consts
from splunk_ta_mscs_i2.models import (
    ProxyConfig,
    AzureAccountConfig,
    AzureStorageAccountConfig,
    format_validation_exception,
)
from mscs_storage_service import (
    _create_blob_checkpoint_store_service,
)
from splunk_ta_mscs_i2.mscs_credential_provider import get_credential
from solnlib.file_monitor import FileChangesChecker

logger = logging.get_module_logger()


class LoadBalancingStrategyField(Field):
    def __init__(self, key, **kwargs):
        super().__init__(key, **kwargs)

    @staticmethod
    def _is_valid_enum_value(value):
        return any(value == strategy.value for strategy in LoadBalancingStrategy)

    def parse(self, document) -> LoadBalancingStrategy:
        if self._key not in document:
            return self._default
        strategy = document.get(self._key)
        if not self._is_valid_enum_value(strategy):
            logger.warning(
                "Invalid value '{}' for load balancing strategy, using default value".format(
                    strategy
                )
            )
            return self._default
        return LoadBalancingStrategy(strategy)


class FileMonitor:
    def __init__(
        self,
        callback: Callable[[List[str]], Any],
        files_relative_paths: List,
        interval: datetime.timedelta,
    ):
        app_path = self._get_app_path()
        files_absolute_paths = [
            path.join(app_path, file_relative_path)
            for file_relative_path in files_relative_paths
        ]
        self._checker = FileChangesChecker(callback, files_absolute_paths)
        self._interval = interval
        self._update_check_time()

    def check(self):
        cur_time = datetime.datetime.now(datetime.timezone.utc)
        if cur_time < self._next_file_check_time:
            return

        self._update_check_time()
        if self._checker.check_changes():
            logger.info("Files changed, executing FileMonitor's callback")

    def _update_check_time(self):
        self._next_file_check_time = (
            datetime.datetime.now(datetime.timezone.utc) + self._interval
        )

    @staticmethod
    def _get_app_path():
        absolute_path = path.abspath(__file__)

        marker = os.path.join(os.path.sep, "etc", "apps")

        logger.debug(
            "Getting app path for FileMonitor",
            absolute_path=absolute_path,
            marker=marker,
        )

        start = absolute_path.rfind(marker)
        if start == -1:
            start = 0
        end = absolute_path.find("lib", start)
        if end == -1:
            return None
        end = end - 1
        app_path = absolute_path[:end]
        return app_path


class FileLock:
    """
    A FileLock that synchronizes the access to a shared file from multiple processes.

    This class leverages `os.lockf` on POSIX compatible system and `msvcrt.locking` on Windows.
    it has also implemented the interface of Context Manager for convenience.
    """

    def __init__(self, fd, size):
        self._fd = fd
        self._size = size

    @classmethod
    def _ms_locking(cls, fd, size, lock):
        import msvcrt

        flag = msvcrt.LK_LOCK if lock else msvcrt.LK_UNLCK
        msvcrt.locking(fd, flag, size)

    @classmethod
    def _posix_locking(cls, fd, size, lock):
        flag = os.F_LOCK if lock else os.F_ULOCK
        os.lockf(fd, flag, size)

    _locking = _ms_locking if os.name == "nt" else _posix_locking

    def __enter__(self):
        os.lseek(self._fd, 0, os.SEEK_SET)
        self._locking(self._fd, self._size, True)

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.fsync(self._fd)
        os.lseek(self._fd, 0, os.SEEK_SET)
        self._locking(self._fd, self._size, False)


class SyncBy:
    """
    A handful decorator for synchronize the call to a callee by a coordinator function.
    """

    def __init__(self, coordinator):
        self._coordinator = coordinator

    def __call__(self, callee):
        def _wrapper(this, *args, **kwargs):
            return self._coordinator(this, callee, *args, **kwargs)

        return _wrapper


class CheckpointSize(Enum):
    # File size of checkpointer with that many partitions
    PARTITIONS_64 = 528384
    PARTITIONS_100 = 823296
    PARTITIONS_1024 = 8392704


class SharedLocalCheckpoint:
    # TODO: Refactor to 1 partition per file
    """
    A SharedLocalCheckpoint that is responsible for persisting the changes of ingestion status to a local file.

    The segmented structure makes partial updating has a constant IO cost.
    """

    def __init__(self, fullname):
        self._lock = Lock()
        self._fd = os.open(fullname, os.O_RDWR | os.O_CREAT)
        self._pages_of_header = 1
        self._number_of_partition = 100
        self._pages_per_partition = 2
        self._number_of_page = (
            self._pages_of_header
            + self._number_of_partition * self._pages_per_partition
        )
        # The typical block size of most Linux systems is 4KB
        self._page_size = 4096
        self._padding = b"\x20"
        self._file_size = self._number_of_page * self._page_size
        self._now = time.time
        self._uuid = uuid.uuid4

    def close(self):
        if self._fd is None:
            return
        os.close(self._fd)
        self._fd = None

    def _get_page_index(self, partition_id, slot):
        if 100 > partition_id >= self._number_of_partition:
            self._initialize(force_partitions=100)
        elif 1024 > partition_id >= self._number_of_partition:
            self._initialize(force_partitions=1024)

        assert (
            partition_id < self._number_of_partition
            and slot < self._pages_per_partition
        )
        return self._pages_of_header + self._pages_per_partition * partition_id + slot

    def _write(self, index, data):
        page_size = self._page_size
        data_size = len(data)
        if data_size > page_size:
            raise ValueError("Data doesn't not fit into one page")
        if data_size < page_size:
            data = data + self._padding * (page_size - data_size)
        offset = index * page_size
        os.lseek(self._fd, offset, os.SEEK_SET)
        os.write(self._fd, data)

    def _write_record(self, index, *args):
        record = [str(self._uuid()), int(self._now())]
        record.extend(args)
        data = json.dumps(record, ensure_ascii=True).encode()
        self._write(index, data)
        return record

    def _read(self, index):
        page_size = self._page_size
        pos = index * page_size
        os.lseek(self._fd, pos, os.SEEK_SET)
        return os.read(self._fd, page_size)

    def _read_record(self, index):
        data = self._read(index)
        if not data.startswith(b"["):
            data = b"[]"
        return json.loads(data)

    def _if_match_origin(self, index, origin):
        record = self._read_record(index)
        if not record:
            if not origin:
                return True
            return False
        return record[0] == origin

    def _lock_thread_and_file(self, func, *args, **kwargs):
        with self._lock:
            with FileLock(self._fd, self._file_size):
                return func(self, *args, **kwargs)

    def _set_partition_size(self, number_of_partition):
        self._number_of_partition = number_of_partition
        self._number_of_page = (
            self._pages_of_header
            + self._number_of_partition * self._pages_per_partition
        )
        self._file_size = self._number_of_page * self._page_size

    def _initialize(self, force_partitions=0):
        # To make it backwards compatible I cannot touch the header inside the file
        # or the name of the file, so I will do it from the filesize which is constant
        header = [4096, 1, 2, 64, 1 + 64 * 2]

        try:
            file_size = os.path.getsize(self._fd)

            # The ratio over the file_size is just to make sure OS won't make any weird things
            if file_size > 0.6 * CheckpointSize.PARTITIONS_1024.value:
                # 1024 partition is the biggest, so we need to extend file
                self._set_partition_size(1024)
                return
            elif (
                0.9 * CheckpointSize.PARTITIONS_100.value
                < file_size
                < 0.6 * CheckpointSize.PARTITIONS_1024.value
            ):
                if force_partitions == 1024:
                    # File is 100p, but needs to be extended to 1024
                    self._set_partition_size(1024)
            elif 0 < file_size < 0.9 * CheckpointSize.PARTITIONS_100.value:
                if force_partitions == 100:
                    # File is 64p, but we need to extend it to 100p
                    self._set_partition_size(100)
                else:
                    # 64 partition is the lowest, so do not truncate it
                    self._set_partition_size(64)
                    return
        except OSError:  # If the file doesn't exist
            logger.debug("EventHub checkpointer doesn't exist")
            pass

        page_size = self._page_size
        pos = self._number_of_page * page_size
        os.lseek(self._fd, pos, os.SEEK_SET)

        record = self._read_record(0)
        if not record:
            self._write_record(0, *header)

        if not record or force_partitions > 0:
            os.ftruncate(self._fd, self._file_size)

    @SyncBy(_lock_thread_and_file)
    def initialize(self, force_max_partitions=False):
        # Taken out, as twice SyncBy breaks it completely
        self._initialize(force_max_partitions)

    @SyncBy(_lock_thread_and_file)
    def read_partition_ownerships(self):
        result = []
        for i in range(self._number_of_partition):
            index = self._get_page_index(i, 0)
            ownership = self._read_record(index)
            if not ownership:
                continue
            result.append([i, ownership])
        return result

    @SyncBy(_lock_thread_and_file)
    def update_partition_ownership(self, partition_id, owner_id, origin):
        index = self._get_page_index(partition_id, 0)
        if self._if_match_origin(index, origin):
            return self._write_record(index, owner_id)
        return []

    @SyncBy(_lock_thread_and_file)
    def read_partition_checkpoints(self):
        result = []
        for i in range(self._number_of_partition):
            index = self._get_page_index(i, 1)
            checkpoint = self._read_record(index)
            if not checkpoint:
                continue
            result.append([i, checkpoint])
        return result

    @SyncBy(_lock_thread_and_file)
    def update_partition_checkpoint(self, partition_id, offset, sequence_number):
        index = self._get_page_index(partition_id, 1)
        return self._write_record(index, offset, sequence_number)


class LocalFileCheckpointStore(CheckpointStore):
    """
    This class is an implementation of `azure.eventhub.CheckpointStore`.

    It uses SharedLocalCheckpoint to store the partition ownership and checkpoint data.
    """

    @classmethod
    def open(cls, workspace, fully_qualified_namespace, eventhub_name, consumer_group):
        urn = [fully_qualified_namespace, eventhub_name, consumer_group]
        filename = "-".join(urn) + ".v1.ckpt"
        fullname = path.join(workspace, filename)
        checkpoint = SharedLocalCheckpoint(fullname)
        checkpoint.initialize()
        return cls(*urn, checkpoint=checkpoint)

    def __init__(
        self, fully_qualified_namespace, eventhub_name, consumer_group, checkpoint
    ):
        self._fully_qualified_namespace = fully_qualified_namespace
        self._eventhub_name = eventhub_name
        self._consumer_group = consumer_group
        self._checkpoint = checkpoint

    def close(self):
        self._checkpoint.close()

    def _validate_source(
        self, fully_qualified_namespace, eventhub_name, consumer_group
    ):
        return all(
            [
                self._fully_qualified_namespace == fully_qualified_namespace,
                self._eventhub_name == eventhub_name,
                self._consumer_group == consumer_group,
            ]
        )

    def list_ownership(self, fully_qualified_namespace, eventhub_name, consumer_group):
        assert self._validate_source(
            fully_qualified_namespace, eventhub_name, consumer_group
        )

        result = []
        ownership_list = self._checkpoint.read_partition_ownerships()
        for partition_id, ownership in ownership_list:
            etag, last_modified, owner_id = ownership
            result.append(
                {
                    "fully_qualified_namespace": fully_qualified_namespace,
                    "eventhub_name": eventhub_name,
                    "consumer_group": consumer_group,
                    "partition_id": str(partition_id),
                    "owner_id": owner_id,
                    "etag": etag,
                    "last_modified_time": last_modified,
                }
            )
        return result

    def claim_ownership(self, ownership_list):
        ownership_acquired = []
        for ownership in ownership_list:
            assert self._validate_source(
                *[
                    ownership[key]
                    for key in [
                        "fully_qualified_namespace",
                        "eventhub_name",
                        "consumer_group",
                    ]
                ]
            )

            partition_id = int(ownership["partition_id"])
            owner_id = ownership["owner_id"]
            etag = ownership.get("etag")

            record = self._checkpoint.update_partition_ownership(
                partition_id, owner_id, etag
            )
            if not record:
                continue
            ownership["etag"] = record[0]
            ownership["last_modified_time"] = record[1]
            ownership_acquired.append(ownership)

        return ownership_acquired

    def list_checkpoints(
        self, fully_qualified_namespace, eventhub_name, consumer_group
    ):
        assert self._validate_source(
            fully_qualified_namespace, eventhub_name, consumer_group
        )

        result = []
        checkpoint_list = self._checkpoint.read_partition_checkpoints()
        for partition_id, checkpoint in checkpoint_list:
            _, _, offset, sequence_number = checkpoint
            result.append(
                {
                    "fully_qualified_namespace": fully_qualified_namespace,
                    "eventhub_name": eventhub_name,
                    "consumer_group": consumer_group,
                    "partition_id": str(partition_id),
                    "offset": offset,
                    "sequence_number": sequence_number,
                }
            )
        return result

    def update_checkpoint(self, checkpoint):
        assert self._validate_source(
            *[
                checkpoint[key]
                for key in [
                    "fully_qualified_namespace",
                    "eventhub_name",
                    "consumer_group",
                ]
            ]
        )
        partition_id = int(checkpoint["partition_id"])
        offset = checkpoint["offset"]
        sequence_number = checkpoint["sequence_number"]
        self._checkpoint.update_partition_checkpoint(
            partition_id, offset, sequence_number
        )


class SettingsConfiguration:
    """
    This utility class is responsible for holding global settings across the full life cycle of an input.
    """

    @classmethod
    def load(cls, config):
        """Loads MSCS settings."""
        logging_content = config.load(
            "splunk_ta_mscs_i2_settings", stanza="logging", use_cred=True
        )
        logging_parser = StanzaParser(
            [LogLevelField(mscs_consts.AGENT, default="INFO")]
        )
        logging_settings = logging_parser.parse(logging_content)

        performance_tuning_settings_content = config.load(
            "splunk_ta_mscs_i2_settings",
            stanza=mscs_consts.PERFORMANCE_TUNING_SETTINGS,
            use_cred=True,
        )
        performance_tuning_settings_parser = StanzaParser(
            [
                BooleanField(
                    mscs_consts.SKIP_FILES_MONITORING_FOR_RESTART, default=False
                ),
                BooleanField(mscs_consts.EVENT_HUB_WATCHDOG_ENABLED, default=False),
                LoadBalancingStrategyField(
                    mscs_consts.LOAD_BALANCING_STRATEGY,
                    default=LoadBalancingStrategy.BALANCED,
                ),
            ]
        )
        performance_tuning_settings = performance_tuning_settings_parser.parse(
            performance_tuning_settings_content
        )

        return cls(logging_settings, performance_tuning_settings)

    def __init__(self, logging_settings, performance_tuning_settings):
        self._logging = logging_settings
        self._performance_tuning_settings = performance_tuning_settings

    def get_log_level(self):
        """Get log level."""
        return self._logging.agent

    def get_skip_files_monitoring_for_restart(self):
        return self._performance_tuning_settings.skip_files_monitoring_for_restart

    def get_load_balancing_strategy(self):
        return self._performance_tuning_settings.load_balancing_strategy

    def get_event_hub_watchdog_enabled(self):
        return self._performance_tuning_settings.event_hub_watchdog_enabled


@dataclass(frozen=True)
class WatchdogPartitionSnapshot:
    partition_id: str
    generation: int


@dataclass(frozen=True)
class WatchdogStall:
    partition_id: str
    processed_sequence_number: Optional[int]
    remote_sequence_number: int
    stalled_checks: int


@dataclass
class _PartitionProgress:
    generation: int
    processed_sequence_number: Optional[int] = None
    stalled_checks: int = 0
    batch_in_progress: bool = False


class EventHubProgressWatchdog:
    """Tracks partition progress without treating an empty stream as a stall."""

    def __init__(
        self,
        enabled: bool,
        interval_seconds: int = mscs_consts.EVENT_HUB_WATCHDOG_INTERVAL_SECONDS,
        stalled_checks: int = mscs_consts.EVENT_HUB_WATCHDOG_STALLED_CHECKS,
        clock: Callable[[], float] = time.monotonic,
    ):
        self._enabled = enabled
        self._interval_seconds = interval_seconds
        self._required_stalled_checks = stalled_checks
        self._clock = clock
        self._next_check = clock() + interval_seconds
        self._lock = Lock()
        self._next_generation = 0
        self._partitions: Dict[str, _PartitionProgress] = {}
        self._has_processed_events = False

    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def has_processed_events(self) -> bool:
        with self._lock:
            return self._has_processed_events

    def is_due(self) -> bool:
        if not self._enabled:
            return False

        now = self._clock()
        with self._lock:
            if now < self._next_check:
                return False
            self._next_check = now + self._interval_seconds
            return True

    def partition_initialized(
        self, partition_id: str, checkpoint_sequence_number: Optional[int] = None
    ) -> None:
        if not self._enabled:
            return
        with self._lock:
            self._next_generation += 1
            self._partitions[partition_id] = _PartitionProgress(
                generation=self._next_generation,
                processed_sequence_number=checkpoint_sequence_number,
            )

    def partition_closed(self, partition_id: str) -> None:
        if not self._enabled:
            return
        with self._lock:
            self._partitions.pop(partition_id, None)

    def batch_started(self, partition_id: str) -> None:
        with self._lock:
            progress = self._partitions.get(partition_id)
            if progress:
                progress.batch_in_progress = True

    def batch_completed(
        self, partition_id: str, sequence_number: Optional[int]
    ) -> None:
        with self._lock:
            self._has_processed_events = True
            progress = self._partitions.get(partition_id)
            if not progress:
                return
            progress.batch_in_progress = False
            progress.stalled_checks = 0
            if sequence_number is None:
                return
            progress.processed_sequence_number = sequence_number

    def batch_failed(self, partition_id: str) -> None:
        with self._lock:
            progress = self._partitions.get(partition_id)
            if progress:
                progress.batch_in_progress = False

    def snapshots(self) -> List[WatchdogPartitionSnapshot]:
        with self._lock:
            return [
                WatchdogPartitionSnapshot(partition_id, progress.generation)
                for partition_id, progress in self._partitions.items()
            ]

    def observe(
        self,
        snapshot: WatchdogPartitionSnapshot,
        remote_sequence_number: int,
        partition_is_empty: bool,
    ) -> Optional[WatchdogStall]:
        with self._lock:
            progress = self._partitions.get(snapshot.partition_id)
            if not progress or progress.generation != snapshot.generation:
                return None
            return self._observe_progress(
                snapshot.partition_id,
                progress,
                remote_sequence_number,
                partition_is_empty,
            )

    def _observe_progress(
        self,
        partition_id: str,
        progress: _PartitionProgress,
        remote_sequence_number: int,
        partition_is_empty: bool,
    ) -> Optional[WatchdogStall]:
        if progress.batch_in_progress:
            progress.stalled_checks = 0
            return None

        local_sequence_number = progress.processed_sequence_number
        if local_sequence_number is None:
            if partition_is_empty:
                progress.stalled_checks = 0
                return None
        elif remote_sequence_number <= local_sequence_number:
            progress.stalled_checks = 0
            return None

        progress.stalled_checks += 1
        logger.debug(
            "Event Hub watchdog observed pending events without local ingestion progress.",
            partition_id=partition_id,
            processed_sequence_number=local_sequence_number,
            remote_sequence_number=remote_sequence_number,
            sequence_backlog=(
                remote_sequence_number - local_sequence_number
                if local_sequence_number is not None
                else None
            ),
            partition_is_empty=partition_is_empty,
            stalled_checks=progress.stalled_checks,
            stalled_check_threshold=self._required_stalled_checks,
        )
        if progress.stalled_checks < self._required_stalled_checks:
            return None
        return WatchdogStall(
            partition_id=partition_id,
            processed_sequence_number=local_sequence_number,
            remote_sequence_number=remote_sequence_number,
            stalled_checks=progress.stalled_checks,
        )


class EventHubConsumerHandler:
    WithSystemProperties = 1
    WithPartitionID = 2
    WithConsumerGroup = 4

    """
    This class bridges events from an `EventHubConsumerClient` to a Splunk event writer.
    """

    def __init__(
        self,
        event_hub_consumer,
        event_writer,
        max_wait_time,  # type: int
        max_batch_size,  # type: int
        event_format_flags,  # type: int
        ensure_ascii,  # type: bool
        eventhub_namespace,  # type: str
        eventhub_name,  # type: str
        stanza_name,  # type: str
        watchdog_enabled=False,  # type: bool
        checkpoint_sequence_number_provider=None,  # type: Optional[Callable[[str], Optional[int]]]
    ):
        self._event_hub_consumer = event_hub_consumer
        self._event_writer = event_writer
        self._shutdown_event = Event()
        self._thread = Thread(target=self._work_proc, daemon=True)
        self._max_wait_time = max_wait_time
        self._max_batch_size = max_batch_size
        self._event_format_flags = event_format_flags
        self._ensure_ascii = ensure_ascii
        self._encoding = "utf-8"
        self._main_context = logging.ThreadLocalLoggingStack.top()
        self._next_logging_time = datetime.datetime.now(
            datetime.timezone.utc
        ) + datetime.timedelta(minutes=5)
        # Dictionary to store event counts for each partition
        self._partition_event_counts = {}
        self.event_not_json = 0
        self._eventhub_namespace = eventhub_namespace
        self._eventhub_name = eventhub_name
        self._stanza_name = stanza_name
        self._watchdog = EventHubProgressWatchdog(watchdog_enabled)
        self._checkpoint_sequence_number_provider = checkpoint_sequence_number_provider
        self._watchdog_restart_requested = False
        self._transport_restart_requested = False
        self._worker_stop_failed = False

        self._obs = StanzaObservabilityRecorder("event-hub", logger, stanza_name)
        self._event_latency_histogram = self._obs.register_instrument(
            lambda meter: meter.create_histogram(
                name="eventhub.events.latency",
                description="Time difference between event enqueued time and processing time",
                unit="s",
            )
        )

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._close_event_hub_consumer()
        self._thread.join(mscs_consts.EVENT_HUB_CONSUMER_CLOSE_JOIN_TIMEOUT_SECONDS)
        if self._thread.is_alive():
            self._worker_stop_failed = True
            logger.error(
                "EventHubConsumerClient worker thread did not stop after close.",
                stanza_name=self._stanza_name,
            )
            # The worker thread may still be running a receive callback and
            # mutating partition counters/observability state, so leave that
            # state alone rather than tearing it down underneath it.
            return
        self._log_partition_events()
        self._obs.flush()

    def is_alive(self):
        return self._event_hub_consumer is not None and self._thread.is_alive()

    @property
    def worker_stop_failed(self) -> bool:
        return self._worker_stop_failed

    @property
    def watchdog(self) -> EventHubProgressWatchdog:
        return self._watchdog

    @property
    def watchdog_restart_requested(self) -> bool:
        return self._watchdog_restart_requested

    @property
    def restart_requested(self) -> bool:
        return self._watchdog_restart_requested or self._transport_restart_requested

    @property
    def has_processed_events(self) -> bool:
        return self._watchdog.has_processed_events

    def _has_set(self, flag):
        return self._event_format_flags & flag

    def _format_source(self, context: PartitionContext):
        params = {
            "endpoint": "eventhub://{namespace}/{entity};".format(
                namespace=context.fully_qualified_namespace,
                entity=context.eventhub_name,
            ),
            "partition_id": "partition_id={};".format(context.partition_id)
            if self._has_set(self.WithPartitionID)
            else "",
            "consumer_group": "consumer_group={};".format(context.consumer_group)
            if self._has_set(self.WithConsumerGroup)
            else "",
        }
        source = "{endpoint}{partition_id}{consumer_group}".format(**params)
        return source

    def _format_json_pair(self, key, value):
        key = json.dumps(key, ensure_ascii=self._ensure_ascii)
        if not isinstance(value, str):
            value = json.dumps(value, ensure_ascii=self._ensure_ascii)
        elif not value.startswith("{") or not value.endswith("}"):
            value = json.dumps(value, ensure_ascii=self._ensure_ascii)
        return "{}:{}".format(key, value)

    @staticmethod
    def _normalize_event(event):
        try:
            body = event.body
            if isinstance(body, bytes):
                return [body]
            return [line for line in body]
        except ValueError:
            logger.warning("The event content is empty.")
        return []

    def _decode_event(self, event):
        for line in self._normalize_event(event):
            try:
                yield line.decode(self._encoding)
            except UnicodeDecodeError:
                logger.warning("An error occured during decoding the event.")
                yield line.hex()

    def _format_event(self, event, body):
        if self._has_set(self.WithSystemProperties):
            pairs = [self._format_json_pair("body", body)]
            for props in [event.system_properties, event.properties]:
                if not props:
                    continue
                for key, value in props.items():
                    key = key.decode() if isinstance(key, bytes) else key
                    value = (
                        value.decode("utf-8", "backslashreplace")
                        if isinstance(value, bytes)
                        else value
                    )
                    pairs.append(self._format_json_pair(key, value))
            return "".join(["{", ",".join(pairs), "}"])
        elif isinstance(body, str):
            return body
        else:
            return json.dumps(body, ensure_ascii=self._ensure_ascii)

    def _select_elements(self, source, event):
        event_count = 0
        decoded_events = "".join([line for line in self._decode_event(event)])
        is_event_body_json = False
        volume = 0
        try:
            event_body = json.loads(decoded_events)
            is_event_body_json = True
        except ValueError:
            self.event_not_json = self.event_not_json + 1

        if is_event_body_json and isinstance(event_body, dict):
            if "records" in event_body and isinstance(
                event_body["records"], (list, set, tuple)
            ):
                for val in event_body["records"]:
                    data = self._format_event(event, val)
                    volume += self._event_writer.write_fileobj(data, source=source)
                    event_count += 1
            else:
                data = self._format_event(event, event_body)
                volume = self._event_writer.write_fileobj(data, source=source)
                event_count += 1
        else:
            data = self._format_event(event, decoded_events)
            volume = self._event_writer.write_fileobj(data, source=source)
            event_count += 1
        return event_count, volume

    @staticmethod
    def _parse_partition_context(context):
        if context is None:
            return {}
        return {
            key: value
            for key, value in vars(context).items()
            if not key.startswith("_")
        }

    def _on_event_batch(self, context: PartitionContext, event_batch):
        current_partition = context.partition_id
        if current_partition not in self._partition_event_counts:
            self._partition_event_counts[current_partition] = {
                "event_count": 0,
                "event_volume": 0,
            }

        if not event_batch:
            self._log_events_periodically()
            return

        self._watchdog.batch_started(current_partition)
        try:
            self._ingest_event_batch(context, event_batch)
            context.update_checkpoint()
        except Exception:
            self._watchdog.batch_failed(current_partition)
            raise

        sequence_number = getattr(event_batch[-1], "sequence_number", None)
        self._watchdog.batch_completed(current_partition, sequence_number)
        self._log_events_periodically()

    def _ingest_event_batch(self, context: PartitionContext, event_batch) -> None:
        current_partition = context.partition_id
        source = self._format_source(context)
        for event in event_batch:
            event_count, event_volume = self._select_elements(source, event)
            self._partition_event_counts[current_partition][
                "event_count"
            ] += event_count
            self._partition_event_counts[current_partition][
                "event_volume"
            ] += event_volume
            self._record_event_latency(current_partition, event)

    def _record_event_latency(self, partition_id: str, event) -> None:
        if not self._event_latency_histogram or not hasattr(event, "enqueued_time"):
            return

        try:
            current_time = datetime.datetime.now(datetime.timezone.utc)
            latency_seconds = (current_time - event.enqueued_time).total_seconds()
            self._event_latency_histogram.record(
                latency_seconds,
                attributes={
                    ATTR_MODINPUT_NAME: self._stanza_name,
                    "partition.id": str(partition_id),
                },
            )
        except Exception as exc:
            logger.debug(f"Failed to record event latency: {exc}")

    def _log_events_periodically(self, is_aborted=False):
        if is_aborted:
            self._log_partition_events()
        else:
            cur_time = datetime.datetime.now(datetime.timezone.utc)
            if cur_time >= self._next_logging_time:
                self._log_partition_events()
                self._next_logging_time = cur_time + datetime.timedelta(minutes=1)

    def _log_partition_events(self):
        for partition, data in self._partition_event_counts.items():
            event_count = data["event_count"]
            event_volume = data["event_volume"]

            # Record OpenTelemetry metrics
            self._obs.record(
                event_count, event_volume, extra_attrs={"partition.id": partition}
            )

            logger.info(
                "Ingested events. ",
                partition_id=partition,
                event_count=event_count,
                event_volume=event_volume,
            )

        if self.event_not_json > 0:
            logger.debug(
                f"Splunk Event message body is not JSON - parsing as text. Times: {self.event_not_json}"
            )
            self.event_not_json = 0

        self._partition_event_counts.clear()

    def restart_for_watchdog(self, stall: WatchdogStall) -> None:
        if self._watchdog_restart_requested:
            return

        self._watchdog_restart_requested = True
        local_sequence_number = stall.processed_sequence_number
        logger.warning(
            "Recreating stalled EventHubConsumerClient after Azure reported "
            "pending events without local ingestion progress.",
            partition_id=stall.partition_id,
            processed_sequence_number=local_sequence_number,
            remote_sequence_number=stall.remote_sequence_number,
            sequence_backlog=(
                stall.remote_sequence_number - local_sequence_number
                if local_sequence_number is not None
                else None
            ),
            stalled_checks=stall.stalled_checks,
            eventhub_namespace=self._eventhub_namespace,
            eventhub_name=self._eventhub_name,
            stanza_name=self._stanza_name,
        )
        self._close_event_hub_consumer()

    def _close_event_hub_consumer(self):
        event_hub_consumer = self._event_hub_consumer
        self._event_hub_consumer = None
        self._shutdown_event.set()
        if not event_hub_consumer:
            return

        try:
            event_hub_consumer.close()
        except Exception as exc:
            logger.warning(
                "Error closing EventHubConsumerClient.",
                stanza_name=self._stanza_name,
                exc_info=exc,
            )

    @property
    def main_context(self):
        return self._main_context

    def _on_error(self, context: PartitionContext, error):
        partition_ctx = self._parse_partition_context(context)

        if isinstance(error, AuthenticationError):
            if EventHubDataInput._is_transient_auth_error(error):
                logger.warning(
                    "Transient CBS authentication error during receive. "
                    "The SDK will retry and recreate the partition consumer.",
                    exc_info=error,
                    **partition_ctx,
                )
            else:
                logger.error(
                    "Authentication error during "
                    "receive. Check credentials and permissions.",
                    exc_info=error,
                    **partition_ctx,
                )
        else:
            logger.error(
                "An error occurred during receiving events.",
                exc_info=error,
                **partition_ctx,
            )

    @LogWith(prefix=main_context)
    def _on_partition_initialize(self, context: PartitionContext):
        checkpoint_sequence_number = None
        if self._watchdog.enabled and self._checkpoint_sequence_number_provider:
            checkpoint_sequence_number = self._checkpoint_sequence_number_provider(
                context.partition_id
            )
        self._watchdog.partition_initialized(
            context.partition_id, checkpoint_sequence_number
        )
        logger.info("Partition initialized.", **self._parse_partition_context(context))

    def _on_partition_close(self, context: PartitionContext, close_reason: CloseReason):
        self._watchdog.partition_closed(context.partition_id)
        logger.info(
            "Partition closed.",
            close_reason=close_reason,
            **self._parse_partition_context(context),
        )

    @LogWith(prefix=main_context)
    def _work_proc(self):

        logger.info(
            "Start collecting events.",
            max_wait_time=self._max_wait_time,
            max_batch_size=self._max_batch_size,
        )
        consecutive_auth_failures = 0
        while True:
            event_hub_consumer = self._event_hub_consumer
            if not event_hub_consumer:
                break
            try:
                event_hub_consumer.receive_batch(
                    on_event_batch=self._on_event_batch,
                    max_wait_time=self._max_wait_time,
                    max_batch_size=self._max_batch_size,
                    # "-1" is from the beginning of the partition.
                    starting_position="-1",
                    on_error=self._on_error,
                    on_partition_initialize=self._on_partition_initialize,
                    on_partition_close=self._on_partition_close,
                )
                consecutive_auth_failures = 0
            except AuthenticationError as e:
                if EventHubDataInput._is_transient_auth_error(e):
                    consecutive_auth_failures += 1
                    if consecutive_auth_failures >= mscs_consts.MAX_AUTH_RETRIES:
                        self._transport_restart_requested = True
                        logger.error(
                            "receive_batch authentication failed after %d "
                            "consecutive transient errors. Stopping receive loop.",
                            consecutive_auth_failures,
                            exc_info=e,
                        )
                        break
                    delay = EventHubDataInput._calculate_backoff_delay(
                        consecutive_auth_failures - 1,
                        mscs_consts.AUTH_RETRY_INITIAL_BACKOFF_SECONDS,
                        mscs_consts.AUTH_RETRY_MAX_BACKOFF_SECONDS,
                    )
                    logger.warning(
                        "Transient CBS auth error on receive_batch "
                        "(attempt %d/%d). Retrying in %.1fs.",
                        consecutive_auth_failures,
                        mscs_consts.MAX_AUTH_RETRIES,
                        delay,
                        exc_info=e,
                    )
                    if self._shutdown_event.wait(delay):
                        break
                else:
                    logger.error(
                        "Authentication error on "
                        "receive_batch. Stopping receive loop. "
                        "Check credentials and permissions.",
                        exc_info=e,
                    )
                    break
            except Exception as exc:
                self._transport_restart_requested = True
                logger.error(
                    "receive_batch stopped after an Event Hub transport error.",
                    stanza_name=self._stanza_name,
                    exc_info=exc,
                )
                break


class EventHubDataInput:
    """
    The facade class for an instance of Azure Event Hub input.

    Simply creating components with corresponding arguments and wire them up all together.
    """

    # TODO: Refactor the structure of EventHub modinput, why is everything JIT processed and merged together

    def __init__(self, stanza):
        self._kind = stanza.kind
        self._name = stanza.name
        self._args = stanza.content
        self._start_time = int(time.time())

    @property
    def name(self):
        return self._name

    @property
    def start_time(self):
        return self._start_time

    def _extract_arguments(self, parser):
        return parser.parse(self._args)

    def _get_account_name(self):
        parser = StanzaParser(
            [
                StringField("account", required=True),
            ]
        )
        args = self._extract_arguments(parser)
        return args.account

    @staticmethod
    def _get_https_scheme(proxy: ProxyConfig) -> Dict[str, Optional[str]]:
        if not proxy.enabled:
            return {}
        return {"https": proxy.url}

    def _create_credentials(
        self, config: ConfigManager, proxy: ProxyConfig
    ) -> ClientSecretCredential:
        proxies = self._get_https_scheme(proxy)
        account_name = self._get_account_name()
        content = config.load(
            "splunk_ta_mscs_i2_azureaccount",
            stanza=account_name,
            virtual=True,
            use_cred=True,
        )
        account = AzureAccountConfig.from_dict(content)
        credential = get_credential(account, proxies, logger)
        return credential

    def _try_creating_blob_checkpoint_store(
        self, config: ConfigManager, proxy: ProxyConfig
    ):
        parser = StanzaParser(
            [
                BooleanField("blob_checkpoint_enabled", default=False),
                StringField("storage_account", default=""),
                StringField("container_name", default=""),
            ]
        )
        args = self._extract_arguments(parser)
        if not args.blob_checkpoint_enabled:
            logger.info("Blob checkpoint store not configured")
            return None

        logger.info("Blob checkpoint store has been configured")
        logger.debug(" container_name: " + str(args.container_name))

        if not (args.storage_account and args.container_name):
            logger.warning(
                "Either Storage Account or Storage Container Name not provided for Blob "
                "Checkpointing"
            )
            return None

        try:
            storage_account_name = args.storage_account
            content_storage = config.load(
                "splunk_ta_mscs_i2_storageaccount",
                stanza=storage_account_name,
                virtual=True,
                use_cred=True,
            )
            storage_account = AzureStorageAccountConfig.from_dict(content_storage)
            proxies = self._get_https_scheme(proxy)

            blob_service_checkpoint = _create_blob_checkpoint_store_service(
                storage_account=storage_account,
                container_name=args.container_name,
                proxies=proxies,
            )
            return blob_service_checkpoint
        except ClassValidationError as e:
            logger.error(
                f"Failed to validate Azure Account model for the account: {storage_account_name}. Error details: {transform_error(e, format_exception=format_validation_exception)}",
                exc_info=e,
            )
            return None
        except Exception as e:
            logger.error(
                f"Unknown exception occurred while getting Azure Storage Account from config: {e}",
                exc_info=e,
            )
            return None

    def _extract_transport_type(self):
        parser = StanzaParser(
            [BooleanField("use_amqp_over_websocket", default=True, rename="aow")]
        )
        args = self._extract_arguments(parser)
        if args.aow:
            return TransportType.AmqpOverWebsocket
        return TransportType.Amqp

    def _extract_force_amqp_over_proxy(self):
        parser = StanzaParser(
            [BooleanField("force_amqp_over_proxy", default=False, rename="aop")]
        )
        args = self._extract_arguments(parser)
        return bool(args.aop)

    def _extract_ensure_ascii(self):
        """Backwards compatibility requires ensure_ascii to be True, while in UI it defaults False"""
        parser = StanzaParser([BooleanField("ensure_ascii", default=True, rename="ea")])
        args = self._extract_arguments(parser)
        return bool(args.ea)

    def isExists(
        self, workspace, fully_qualified_namespace, eventhub_name, consumer_group
    ):
        urn = [fully_qualified_namespace, eventhub_name, consumer_group]
        filename = "-".join(urn) + ".v1.ckpt"
        fullname = path.join(workspace, filename)
        if path.isfile(fullname):
            return True
        return False

    def _create_event_hub_consumer(
        self,
        workspace: str,
        config: ConfigManager,
        credential: ClientSecretCredential,
        proxy: ProxyConfig,
        settings: SettingsConfiguration,
        enable_custom_retry: bool = True,
    ):
        parser = StanzaParser(
            [
                StringField("event_hub_namespace", required=True),
                StringField("event_hub_name", required=True),
                StringField("consumer_group", default="$Default"),
            ]
        )
        args = self._extract_arguments(parser)
        checkpoint_store = self._try_creating_blob_checkpoint_store(config, proxy)

        if not checkpoint_store:
            checkpoint_store = LocalFileCheckpointStore.open(
                workspace,
                args.event_hub_namespace,
                args.event_hub_name,
                args.consumer_group,
            )
        elif not checkpoint_store.list_checkpoints(
            args.event_hub_namespace, args.event_hub_name, args.consumer_group
        ):
            logger.info(" LocalFileCheckpoint file Exists ")

            if self.isExists(
                workspace,
                args.event_hub_namespace,
                args.event_hub_name,
                args.consumer_group,
            ):

                # if checkpoint list is empty then needs to try to migrate.
                checkpoint_store_source = LocalFileCheckpointStore.open(
                    workspace,
                    args.event_hub_namespace,
                    args.event_hub_name,
                    args.consumer_group,
                )
                try:

                    migrate_checkpoints(
                        checkpoint_store_source,
                        checkpoint_store,
                        args.event_hub_namespace,
                        args.event_hub_name,
                        args.consumer_group,
                    )
                except Exception as ex:
                    raise Exception(ex)
                finally:
                    checkpoint_store_source.close()
        else:
            logger.info("checkpoint using blob storage and already migrated")

        http_proxy = None
        transport_type = self._extract_transport_type()
        if proxy.enabled and not (
            transport_type == TransportType.Amqp
            and self._extract_force_amqp_over_proxy()
        ):
            http_proxy = {
                "proxy_hostname": proxy.host,
                "proxy_port": proxy.port,
                "username": proxy.username,
                "password": proxy.password,
            }

        load_balancing_strategy = settings.get_load_balancing_strategy()

        client_kwargs = {
            "credential": credential,
            "http_proxy": http_proxy,
            "checkpoint_store": checkpoint_store,
            "transport_type": transport_type,
            "partition_ownership_expiration_interval": 300,
            "load_balancing_strategy": load_balancing_strategy,
        }
        if enable_custom_retry:
            client_kwargs.update(
                retry_total=mscs_consts.MAX_AUTH_RETRIES,
                retry_backoff_factor=mscs_consts.AUTH_RETRY_INITIAL_BACKOFF_SECONDS,
                retry_backoff_max=mscs_consts.AUTH_RETRY_MAX_BACKOFF_SECONDS,
            )
        else:
            client_kwargs.update(
                retry_total=mscs_consts.EVENT_HUB_CLIENT_RETRY_TOTAL,
                idle_timeout=mscs_consts.EVENT_HUB_IDLE_TIMEOUT_SECONDS,
            )

        client = EventHubConsumerClient(
            args.event_hub_namespace,
            args.event_hub_name,
            args.consumer_group,
            **client_kwargs,
        )
        return client, checkpoint_store

    @staticmethod
    def _calculate_backoff_delay(
        attempt: int, initial_delay: int, max_delay: int
    ) -> float:
        """Calculate delay with exponential backoff and random jitter.

        Uses ±20% jitter to prevent thundering herd during maintenance windows.

        :param attempt: Current retry attempt number (0-based)
        :param initial_delay: Initial delay in seconds
        :param max_delay: Maximum delay cap in seconds
        :return: Delay in seconds with jitter applied
        """
        base_delay = min(initial_delay * (2**attempt), max_delay)
        jitter = base_delay * 0.2 * (2 * random.random() - 1)
        return min(base_delay + jitter, max_delay)

    @staticmethod
    def _is_transient_auth_error(error: AuthenticationError) -> bool:
        """Classify known transient CBS authentication failures by message."""
        error_msg = str(error).lower()

        # Some permanent failures still include the generic
        # "cbs token authentication failed" text, so permanent matches
        # must win before we check the transient patterns.
        if any(
            pattern in error_msg
            for pattern in mscs_consts.PERMANENT_AUTH_ERROR_PATTERNS
        ):
            return False

        return any(
            pattern in error_msg
            for pattern in mscs_consts.TRANSIENT_AUTH_ERROR_PATTERNS
        )

    @staticmethod
    def _retry_with_backoff(
        func: Callable[[], Any],
        max_retries: int = mscs_consts.MAX_AUTH_RETRIES,
        initial_delay: int = mscs_consts.AUTH_RETRY_INITIAL_BACKOFF_SECONDS,
        max_delay: int = mscs_consts.AUTH_RETRY_MAX_BACKOFF_SECONDS,
        input_name: str = "",
    ) -> Any:
        """Retry startup validation calls for transient CBS auth failures."""
        for attempt in range(max_retries):
            try:
                return func()
            except AuthenticationError as auth_exc:
                if not EventHubDataInput._is_transient_auth_error(auth_exc):
                    raise

                if attempt < max_retries - 1:
                    delay = EventHubDataInput._calculate_backoff_delay(
                        attempt, initial_delay, max_delay
                    )
                    logger.warning(
                        "Transient CBS authentication failed, retrying",
                        input_name=input_name,
                        attempt=attempt + 1,
                        max_retries=max_retries,
                        backoff_seconds=round(delay, 2),
                        exc_info=auth_exc,
                    )
                    time.sleep(delay)
                else:
                    raise

    @staticmethod
    def _log_partition_properties(
        client: EventHubConsumerClient, input_name: str = ""
    ) -> None:
        """Log partition properties. Non-blocking on failure."""
        try:
            partition_ids = client.get_partition_ids()
            for partition_id in partition_ids:
                partition_properties = client.get_partition_properties(partition_id)
                logger.info("Partition properties", **partition_properties)
        except Exception as exc:
            logger.warning(
                "Could not retrieve partition properties",
                input_name=input_name,
                exc_info=exc,
            )

    def _create_event_writer(self, app: SimpleCollectorV1):
        stanza = self._kind + "://" + self._name
        parser = StanzaParser(
            [
                StringField("sourcetype", default="mscs:azure:eventhub"),
                StringField("index"),
                StringField("host"),
                StringField("stanza", fillempty=stanza),
            ]
        )
        args = self._extract_arguments(parser)
        return app.create_event_writer(sourcetype=args.sourcetype, index=args.index)

    def _create_eventhub_consumer_handler(
        self,
        consumer,
        event_writer,
        eventhub_namespace,
        eventhub_name,
        stanza_name,
        watchdog_enabled=False,
        checkpoint_sequence_number_provider=None,
    ):
        parser = StanzaParser(
            [
                IntegerField("max_wait_time", default=10, lower=5, upper=20),
                IntegerField("max_batch_size", default=300, lower=150, upper=10000),
                IntegerField("event_format_flags", default=0),
            ]
        )
        args = self._extract_arguments(parser)
        ensure_ascii = self._extract_ensure_ascii()
        return EventHubConsumerHandler(
            consumer,
            event_writer,
            **vars(args),
            ensure_ascii=ensure_ascii,
            eventhub_namespace=eventhub_namespace,
            eventhub_name=eventhub_name,
            stanza_name=stanza_name,
            watchdog_enabled=watchdog_enabled,
            checkpoint_sequence_number_provider=checkpoint_sequence_number_provider,
        )

    @staticmethod
    def _get_files_for_monitoring():
        app_conf_file = path.join("default", "app.conf")
        mscs_settings_file = path.join("local", "splunk_ta_mscs_i2_settings.conf")
        azure_accounts_file = path.join("local", "mscs_azure_accounts.conf")
        passwords_file = path.join("local", "passwords.conf")
        files = [app_conf_file, mscs_settings_file, azure_accounts_file, passwords_file]

        return files

    def _get_file_monitor(
        self, app: SimpleCollectorV1, settings: SettingsConfiguration
    ):
        if settings.get_skip_files_monitoring_for_restart():
            logger.info("Skip monitoring files for restart")
            dump_file_monitor = type("", (object,), {"check": lambda _: None})()
            return dump_file_monitor

        files = self._get_files_for_monitoring()
        file_monitor = FileMonitor(
            lambda _: app.abort(), files, datetime.timedelta(seconds=300)
        )
        return file_monitor

    @staticmethod
    def _make_checkpoint_sequence_number_provider(
        checkpoint_store: CheckpointStore,
        fully_qualified_namespace: str,
        eventhub_name: str,
        consumer_group: str,
        input_name: str,
    ) -> Callable[[str], Optional[int]]:
        """Build a provider that lists checkpoints once per TTL window instead of
        once per partition, avoiding an O(P^2) checkpoint listing for P partitions
        when the load balancer (re)assigns many partitions in a short burst."""
        cache: Dict[str, Optional[int]] = {}
        cached_at = [0.0]

        def provider(partition_id: str) -> Optional[int]:
            now = time.monotonic()
            if now - cached_at[0] >= mscs_consts.EVENT_HUB_CHECKPOINT_CACHE_TTL_SECONDS:
                try:
                    checkpoints = checkpoint_store.list_checkpoints(
                        fully_qualified_namespace, eventhub_name, consumer_group
                    )
                    cache.clear()
                    for checkpoint in checkpoints:
                        sequence_number = checkpoint.get("sequence_number")
                        if isinstance(sequence_number, int):
                            cache[checkpoint.get("partition_id")] = sequence_number
                    cached_at[0] = now
                except Exception as exc:
                    logger.warning(
                        "Event Hub watchdog could not retrieve checkpoint positions.",
                        input_name=input_name,
                        exc_info=exc,
                    )
                    return None
            return cache.get(partition_id)

        return provider

    @staticmethod
    def _log_settings(settings: SettingsConfiguration):
        logger.info(
            "Performance Tuning Settings",
            load_balancing_strategy=settings.get_load_balancing_strategy(),
            skip_files_monitoring_for_restart=settings.get_skip_files_monitoring_for_restart(),
            event_hub_watchdog_enabled=settings.get_event_hub_watchdog_enabled(),
        )

    def _check_event_hub_watchdog(
        self,
        handler: EventHubConsumerHandler,
        probe_consumer: EventHubConsumerClient,
    ) -> Optional[bool]:
        watchdog = handler.watchdog
        if not watchdog.is_due():
            return None

        probe_failed = False
        for snapshot in watchdog.snapshots():
            try:
                properties = probe_consumer.get_partition_properties(
                    snapshot.partition_id
                )
                remote_sequence_number = properties["last_enqueued_sequence_number"]
                partition_is_empty = properties["is_empty"]
            except Exception as exc:
                probe_failed = True
                logger.warning(
                    "Event Hub watchdog could not retrieve partition properties.",
                    partition_id=snapshot.partition_id,
                    stanza_name=self._name,
                    exc_info=exc,
                )
                continue

            if not isinstance(remote_sequence_number, int):
                probe_failed = True
                logger.warning(
                    "Event Hub watchdog received an invalid remote sequence number.",
                    partition_id=snapshot.partition_id,
                    stanza_name=self._name,
                    remote_sequence_number=remote_sequence_number,
                )
                continue

            stall = watchdog.observe(
                snapshot, remote_sequence_number, partition_is_empty
            )
            if stall:
                handler.restart_for_watchdog(stall)
                return False

        return probe_failed

    def _monitor_event_hub_consumer(
        self,
        app: SimpleCollectorV1,
        file_monitor: FileMonitor,
        handler: EventHubConsumerHandler,
        probe_consumer: Optional[EventHubConsumerClient],
        probe_consumer_factory: Optional[
            Callable[[], Tuple[EventHubConsumerClient, CheckpointStore]]
        ] = None,
    ) -> Optional[EventHubConsumerClient]:
        probe_failure_count = 0
        with handler:
            while handler.is_alive():
                file_monitor.check()
                if app.is_aborted():
                    break
                if probe_consumer:
                    probe_failed = self._check_event_hub_watchdog(
                        handler, probe_consumer
                    )
                    if probe_failed is True:
                        probe_failure_count += 1
                    elif probe_failed is False:
                        probe_failure_count = 0

                    if (
                        probe_consumer_factory
                        and probe_failure_count
                        >= mscs_consts.EVENT_HUB_WATCHDOG_PROBE_FAILURES_BEFORE_RECREATE
                    ):
                        try:
                            (
                                new_probe_consumer,
                                new_probe_checkpoint_store,
                            ) = probe_consumer_factory()
                        except Exception as exc:
                            logger.warning(
                                "Could not recreate Event Hub watchdog probe "
                                "consumer after partition property failures.",
                                stanza_name=self._name,
                                probe_failure_threshold=mscs_consts.EVENT_HUB_WATCHDOG_PROBE_FAILURES_BEFORE_RECREATE,
                                exc_info=exc,
                            )
                            probe_failure_count = 0
                            continue

                        old_probe_consumer = probe_consumer
                        probe_consumer = new_probe_consumer
                        new_probe_checkpoint_store.close()
                        probe_failure_count = 0
                        try:
                            old_probe_consumer.close()
                        except Exception as exc:
                            logger.warning(
                                "Error closing Event Hub watchdog probe consumer.",
                                stanza_name=self._name,
                                exc_info=exc,
                            )
                        logger.warning(
                            "Recreated Event Hub watchdog probe consumer after "
                            "consecutive partition property failures.",
                            stanza_name=self._name,
                            probe_failure_threshold=mscs_consts.EVENT_HUB_WATCHDOG_PROBE_FAILURES_BEFORE_RECREATE,
                        )
                time.sleep(1.0)
        return probe_consumer

    @staticmethod
    def _wait_before_watchdog_restart(
        app: SimpleCollectorV1, file_monitor: FileMonitor, delay: float
    ) -> bool:
        deadline = time.monotonic() + delay
        while not app.is_aborted():
            file_monitor.check()
            if app.is_aborted():
                return False
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return True
            time.sleep(min(1.0, remaining))
        return False

    def _run_event_hub_consumers(
        self,
        app: SimpleCollectorV1,
        config: ConfigManager,
        workspace: str,
        credential: ClientSecretCredential,
        proxy: ProxyConfig,
        settings: SettingsConfiguration,
        file_monitor: FileMonitor,
        event_writer,
        event_hub_args,
        probe_consumer: Optional[EventHubConsumerClient],
    ) -> None:
        restart_attempt = 0
        try:
            while True:
                try:
                    consumer, checkpoint_store = self._create_event_hub_consumer(
                        workspace,
                        config,
                        credential,
                        proxy,
                        settings,
                        enable_custom_retry=False,
                    )
                except Exception as exc:
                    if app.is_aborted():
                        return
                    logger.error(
                        "Error occurred while creating EventHubConsumerClient.",
                        stanza_name=self._name,
                        exc_info=exc,
                    )
                    delay = self._calculate_backoff_delay(
                        restart_attempt,
                        mscs_consts.EVENT_HUB_WATCHDOG_RESTART_INITIAL_BACKOFF_SECONDS,
                        mscs_consts.EVENT_HUB_WATCHDOG_RESTART_MAX_BACKOFF_SECONDS,
                    )
                    restart_attempt += 1
                    logger.debug(
                        "Waiting before retrying EventHubConsumerClient creation.",
                        stanza_name=self._name,
                        backoff_seconds=round(delay, 2),
                    )
                    if not self._wait_before_watchdog_restart(app, file_monitor, delay):
                        return
                    continue
                consumer_group = getattr(event_hub_args, "consumer_group", "$Default")
                if not isinstance(consumer_group, str):
                    consumer_group = "$Default"

                checkpoint_sequence_number_provider = (
                    self._make_checkpoint_sequence_number_provider(
                        checkpoint_store,
                        event_hub_args.event_hub_namespace,
                        event_hub_args.event_hub_name,
                        consumer_group,
                        self._name,
                    )
                    if settings.get_event_hub_watchdog_enabled()
                    else None
                )

                handler = self._create_eventhub_consumer_handler(
                    consumer,
                    event_writer,
                    event_hub_args.event_hub_namespace,
                    event_hub_args.event_hub_name,
                    stanza_name=self._name,
                    watchdog_enabled=settings.get_event_hub_watchdog_enabled(),
                    checkpoint_sequence_number_provider=checkpoint_sequence_number_provider,
                )
                try:
                    probe_consumer = self._monitor_event_hub_consumer(
                        app,
                        file_monitor,
                        handler,
                        probe_consumer,
                        lambda: self._create_event_hub_consumer(
                            workspace,
                            config,
                            credential,
                            proxy,
                            settings,
                            enable_custom_retry=False,
                        ),
                    )
                except Exception:
                    if not handler.worker_stop_failed:
                        checkpoint_store.close()
                    raise

                if handler.worker_stop_failed is True:
                    app.abort()
                    logger.error(
                        "Aborting Event Hub input after worker thread failed to stop. "
                        "Leaving its checkpoint store open since the worker may still be using it.",
                        stanza_name=self._name,
                    )
                    return
                checkpoint_store.close()
                if not handler.restart_requested or app.is_aborted():
                    return
                if handler.has_processed_events:
                    restart_attempt = 0

                delay = self._calculate_backoff_delay(
                    restart_attempt,
                    mscs_consts.EVENT_HUB_WATCHDOG_RESTART_INITIAL_BACKOFF_SECONDS,
                    mscs_consts.EVENT_HUB_WATCHDOG_RESTART_MAX_BACKOFF_SECONDS,
                )
                restart_attempt += 1
                logger.debug(
                    "Waiting before recreating EventHubConsumerClient.",
                    stanza_name=self._name,
                    backoff_seconds=round(delay, 2),
                )
                if not self._wait_before_watchdog_restart(app, file_monitor, delay):
                    return
        finally:
            if probe_consumer:
                probe_consumer.close()

    @LogWith(datainput=name, start_time=start_time)
    @LogExceptions(
        logger, "Data input was interrupted by an unhandled exception.", lambda e: -1
    )
    def run(self, app: SimpleCollectorV1, config: ConfigManager):
        settings = SettingsConfiguration.load(config)
        logger.setLevel(settings.get_log_level())

        self._log_settings(settings)

        workspace = app.workspace()
        proxy_settings = config.load(
            "splunk_ta_mscs_i2_settings",
            stanza=mscs_consts.PROXY,
            virtual=True,
            use_cred=True,
        )
        proxy = ProxyConfig.from_dict(proxy_settings)
        credential = self._create_credentials(config, proxy)
        file_monitor = self._get_file_monitor(app, settings)
        (
            startup_validation_consumer,
            startup_checkpoint_store,
        ) = self._create_event_hub_consumer(
            workspace,
            config,
            credential,
            proxy,
            settings,
        )

        try:
            self._retry_with_backoff(
                startup_validation_consumer.get_eventhub_properties,
                input_name=self._name,
            )
            self._log_partition_properties(
                startup_validation_consumer,
                input_name=self._name,
            )
        except Exception as exc:
            logger.error(
                "Error occurred while connecting to eventhub",
                input_name=self._name,
                exc_info=exc,
            )
            startup_validation_consumer.close()
            startup_checkpoint_store.close()
            return 0

        startup_validation_consumer.close()
        startup_checkpoint_store.close()

        watchdog_probe_consumer = None
        if settings.get_event_hub_watchdog_enabled():
            try:
                (
                    watchdog_probe_consumer,
                    watchdog_probe_checkpoint_store,
                ) = self._create_event_hub_consumer(
                    workspace,
                    config,
                    credential,
                    proxy,
                    settings,
                    enable_custom_retry=False,
                )
                watchdog_probe_checkpoint_store.close()
            except Exception as exc:
                logger.warning(
                    "Could not create Event Hub watchdog probe consumer. "
                    "Continuing without the watchdog for this run.",
                    stanza_name=self._name,
                    exc_info=exc,
                )

        event_writer = self._create_event_writer(app)

        # Extract EventHub details for metrics
        parser = StanzaParser(
            [
                StringField("event_hub_namespace", required=True),
                StringField("event_hub_name", required=True),
                StringField("consumer_group", default="$Default"),
            ]
        )
        eh_args = self._extract_arguments(parser)

        self._run_event_hub_consumers(
            app,
            config,
            workspace,
            credential,
            proxy,
            settings,
            file_monitor,
            event_writer,
            eh_args,
            watchdog_probe_consumer,
        )
        return 0


def migrate_checkpoints(
    source, destination, fully_qualified_namespace, eventhub_name, consumer_group
):
    """
    migrates a checkpoint from one storage to the other.

    :param  CheckpointStore source
    :param  CheckpointStore destination
    :param str fully_qualified_namespace: The fully qualified namespace that the Event Hub belongs to.
        The format is like "<namespace>.servicebus.windows.net".
    :param str eventhub_name: The name of the specific Event Hub the checkpoints are associated with, relative to
        the Event Hubs namespace that contains it.
    :param str consumer_group: The name of the consumer group the checkpoints are associated with.
    :rtype: Iterable[Dict[str,Any]], Iterable of dictionaries containing partition checkpoint information:

    """
    logger.info("migration: Starting migration of event hub checkpoint")
    if not (source and destination):
        return True

    checkpoints = source.list_checkpoints(
        fully_qualified_namespace, eventhub_name, consumer_group
    )
    logger.info(f"migration: {len(checkpoints)} checkpoints to migrate")

    # exit if there is nothing in the source. Maybe new input
    if len(checkpoints) == 0:
        return True

    # copy each checkpoint to new storage
    for checkpoint in checkpoints:
        logger.debug("migration: update", checkpoint=checkpoint)
        destination.update_checkpoint(checkpoint)
    logger.info("migration: migration complete")
    # will throw exeption if input can not continue.
    return True


def modular_input_run(app: SimpleCollectorV1, config: ConfigManager):
    stanza = app.inputs()[0]
    data_input = EventHubDataInput(stanza)
    return data_input.run(app, config)


def main():
    arguments = {
        "account": {"title": "The Azure Account Name"},
        "event_hub_namespace": {"title": "The Azure Event Hub Namespace (FQDN)"},
        "event_hub_name": {"title": "The Azure Event Hub Name"},
        "consumer_group": {"title": "The Azure Event Hub Consume Group"},
        "blob_checkpoint_enabled": {
            "title": "Enabling EventHub Blob Checkpointing Checkbox"
        },
        "storage_account": {"title": "Storage Account  "},
        "container_name": {
            "title": "Storage Blob Container name for EventHub checkpoint"
        },
        "max_wait_time": {
            "title": "The maximum interval in seconds that the event processor will wait before processing"
        },
        "max_batch_size": {
            "title": "The maximum number of events that would be retrieved in one batch"
        },
        "event_format_flags": {
            "title": "The bitwise flags that determines the format of output events"
        },
        "use_amqp_over_websocket": {
            "title": "The switch that allow using AMQP over WebSocket"
        },
        "ensure_ascii": {
            "title": "The switch that allows enforce ASCII encoding of ingested events"
        },
        "force_amqp_over_proxy": {
            "title": "The switch that allows to override the proxy with AMQP configuration"
        },
        "export_status": {
            "title": "Export status of EventHub modinput to Data Manager"
        },
    }

    SimpleCollectorV1.main(
        modular_input_run,
        title="Azure Event Hub",
        use_single_instance=False,
        arguments=arguments,
    )
