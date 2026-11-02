#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
import os.path as op
import os
import time
import json
from solnlib.modular_input import KVStoreCheckpointer
import splunktaucclib.common.log as stulog
import threading
import hashlib
import platform
import traceback

__CHECKPOINT_DIR_MAX_LEN__ = 180


class MSCSCheckPointPathError(Exception):
    pass


class BaseCheckpointer:
    def close(self, key=None):
        pass

    def format_key(self, key):
        """
        Use sha256() to hash the key to a 64 bits value.
        """
        return hashlib.sha256(key.encode("utf-8")).hexdigest()

    def _wrap_key_state(self, key, state=None):
        """
        The checkpoint file will contain two parts:
            meta: store the original checkpoint name
            data: the real checkpoint content
        """
        new_state = {"meta": key, "data": state}
        return new_state

    def get(self, key):
        stulog.logger.debug(f"getting checkpoint {key} ")
        self._handle_legacy_checkpoint(key)
        value = self._do_get(key)
        return value.get("data") if value else None

    def _do_get(self, key):
        return None

    def update(self, key, state):
        new_value = self._wrap_key_state(key, state)
        self._do_update(key, new_value)

    def _do_update(self, key, state):
        pass

    def delete(self, key):
        pass

    def batch_update(self, states):
        for state in states:
            self.update(state["_key"], state["state"])

    def _handle_legacy_checkpoint(self, key):
        """
        Inherit if needs to migrate old checkpoint systems"""
        pass


class KVCheckpointer(BaseCheckpointer):
    def __init__(
        self,
        meta_configs,
        collection_name="talib_states",
        input_id="mscs_blob_",
    ):
        """
        :meta_configs: dict like and contains checkpoint_dir, session_key,
         server_uri etc
        :app_name: the name of the app
        :collection_name: the collection name to be used.
        Don"t use other method to visit the collection if you are using
         StateStore to visit it.
        """

        # State cache is a dict from _key to value

        self._appname = "Splunk_TA_microsoft-cloudservices_instance_2"
        self._checkpointer = None
        self._collection = collection_name
        stulog.logger.info(f"creating checkpointer {input_id} ")
        self._checkpointer = KVStoreCheckpointer(
            f"MSCS_STORAGE_BLOB_checkpoint_collection_{input_id}",
            meta_configs["session_key"],
            "Splunk_TA_microsoft-cloudservices_instance_2",
        )

    def _get_full_key(self, key):
        return self.format_key(key)

    def _do_update(self, key, state):
        """
        :state: Any JSON serializable
        :return: None if successful, otherwise throws exception
        """
        stulog.logger.debug(f"updating checkpoint {key} ")
        self._checkpointer.update(self._get_full_key(key), {"value": state})

    def _do_get(self, key=None):
        try:
            value = self._checkpointer.get(self._get_full_key(key))
            return value["value"] if value else None
        except Exception as ex:
            return {}

    def delete(self, key=None):
        pass

    def get_formatted_record(self, key, state):
        """
        Formats the record with key and state
        :param key: checkpoint key
        :param state: checkpoint state
        :return record: formatted record with key and state
        """
        new_state = {"value": self._wrap_key_state(key, state)}
        record = {"_key": self.format_key(key), "state": json.dumps(new_state)}
        return record

    def batch_save(self, records):
        """
        batch call to KV Store to update the records
        :param records: list of checkpoints
        """
        self._checkpointer._collection_data.batch_save(*records)


class FileCheckpointer(BaseCheckpointer):
    def __init__(self, checkpoint_dir):
        """
        For windows system, if length of checkpint path is more than 180,
        raise exception and exit.
        """
        self._checkpoint_dir = checkpoint_dir.rstrip(op.sep)
        if (
            platform.system() == "Windows"
            and len(self._checkpoint_dir) >= __CHECKPOINT_DIR_MAX_LEN__
        ):
            msg = "The length of the checkpoint directory path:'{}' is too long. The max length supported is {}".format(
                self._checkpoint_dir, __CHECKPOINT_DIR_MAX_LEN__
            )
            raise MSCSCheckPointPathError(msg)

        if not op.exists(self._checkpoint_dir):
            os.makedirs(self._checkpoint_dir)

    def do_migrate(self, key, existing_ckpt):
        new_key, new_value = self._wrap_key_state(key, existing_ckpt)
        self.do_update(new_key, new_value)

    def _handle_legacy_checkpoint(self, key):
        """
        Migrate old checkpoint files so that all are in the same format.
        It reads the old content out and rewrite with new format.
        If both old and new existing, delete the old one and use the new one.
        """
        old_file_path = op.join(self._checkpoint_dir, key)
        if not os.path.exists(old_file_path):
            return

        new_file_path = op.join(self._checkpoint_dir, self.format_key(key))
        if os.path.exists(new_file_path):
            self._delete_file(old_file_path)
            return

        existing_ckpt = self.do_get(old_file_path)
        if not existing_ckpt:
            self._delete_file(old_file_path)
            return

        stulog.logger.info(
            "Move content of %s to new ckpt file %s", key, self.format_key(key)
        )
        self.do_migrate(key, existing_ckpt)
        self._delete_file(old_file_path)

    def _do_update(self, key, state):
        file_name = op.join(self._checkpoint_dir, self.format_key(key))
        with open(file_name + "_new", "w") as fp:
            json.dump(state, fp)

        retry = 3
        while retry > 0:
            retry -= 1
            try:
                if op.exists(file_name):
                    os.remove(file_name)
                os.rename(file_name + "_new", file_name)
                break
            except OSError as e:
                if retry > 0:
                    stulog.logger.debug(
                        "update checkpoint exception "
                        "retry={}...filename={} "
                        "exception={}".format(retry, file_name, e)
                    )
                    import time

                    time.sleep(1)
                else:
                    stulog.logger.exception(
                        "fail to update checkpoint"
                        "filename={} exception={}".format(file_name, e)
                    )
                    raise e

    def _do_get(self, key):
        file_name = op.join(self._checkpoint_dir, self.format_key(key))
        try:
            with open(file_name, "r") as fp:
                return json.load(fp)
        except (IOError, ValueError):
            return None

    def _delete_file(self, file_path):
        if not os.path.exists(file_path):
            return
        try:
            os.remove(file_path)
        except OSError as e:
            stulog.logger.warning(
                f'message="Failed to clean up deprecated checkpoint file: {file_path}, exc: {e}"',
                exc_info=e,
            )

    def delete(self, key):
        file_names = [
            op.join(self._checkpoint_dir, key),
            op.join(self._checkpoint_dir, self.format_key(key)),
        ]
        stulog.logger.debug("key=%s, file_names=%s", key, file_names)
        for file_name in file_names:
            self._delete_file(file_name)

    def get_checkpoint_dir(self):
        return self._checkpoint_dir


class CachedFileCheckpointer(FileCheckpointer):
    def __init__(self, checkpoint_dir, max_cache_seconds=5):
        super(CachedFileCheckpointer, self).__init__(checkpoint_dir)
        self._states_cache = {}  # item: time, dict
        self._states_cache_lmd = {}  # item: time, dict
        self.max_cache_seconds = max_cache_seconds
        self._lock = threading.Lock()
        self._close = False

    def update(self, key, state):
        with self._lock:
            if self.is_close():
                return
            now = time.time()
            if key in self._states_cache:
                last = self._states_cache_lmd[key][0]
                if now - last >= self.max_cache_seconds:
                    self.update_state_flush(now, key, state)
            else:
                self.update_state_flush(now, key, state)
            update_cached_state = True
            self._states_cache[key] = (
                now,
                state,
                update_cached_state,
            )  # ADDON-21216 - Updated 3rd element in tuple when cache is updated.

    def update_state_flush(self, now, key, state):
        self._states_cache_lmd[key] = (now, state)
        super(CachedFileCheckpointer, self).update(key, state)

    def get(self, key):
        with self._lock:
            if self.is_close():
                return None
            if key in self._states_cache:
                return self._states_cache[key][1]
            else:
                now = time.time()
                state = self.get_from_file(key)
                if state:
                    self._states_cache[key] = now, state
                    self._states_cache_lmd[key] = now, state
                return state

    def get_from_file(self, key):
        return super(CachedFileCheckpointer, self).get(key)

    def delete_state(self, key):
        super(CachedFileCheckpointer, self).delete(key)
        if self._states_cache.get(key):
            del self._states_cache[key]
        if self._states_cache_lmd.get(key):
            del self._states_cache_lmd[key]

    def close(self, key=None):
        with self._lock:
            self._close = True
            if not key:
                for k, cache_content in self._states_cache.items():
                    # If cache_content tuple length is 2 means cache is not updated and no need flush the checkpoint
                    # If cache_content tuple length is 3 means cache is updated and we need to check value(True/False)
                    # in the 3rd element of tuple updated in update() method
                    if len(cache_content) == 2:
                        continue
                    elif len(cache_content) == 3 and cache_content[2]:
                        self.update_state_flush(cache_content[0], k, cache_content[1])
                self._states_cache.clear()
                self._states_cache_lmd.clear()
            elif key in self._states_cache:
                self.update_state_flush(
                    self._states_cache[key][0], key, self._states_cache[key][1]
                )
                del self._states_cache[key]
                del self._states_cache_lmd[key]

    def is_close(self):
        return self._close
