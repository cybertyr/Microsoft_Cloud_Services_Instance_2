#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
from splunk.persistconn.application import PersistentServerConnectionApplication
import sys
import json
from os import path, environ, getcwd
from datetime import datetime, timezone

sys.path = [
    path.join(
        environ.get("SPLUNK_HOME", getcwd()),
        "etc",
        "apps",
        "Splunk_TA_microsoft-cloudservices_instance_2",
        "bin",
    )
] + sys.path
import import_declare_test

from solnlib.utils import is_true
from solnlib.server_info import ServerInfo

from rest_api_interface import get_modinputs, get_checkpoints, flatten_query_params
from export_group import export_groups

CONFIGURATION_PARAMS_TO_SKIP = {
    "eai:acl",
    "eai:access",
    "eai:appName",
    "eai:userName",
    "host_resolved",
    "host",
    "python.version",
    "python.required",
    "start_by_shell",
}


def _get_checkpoints(session_key, modinputs):
    checkpoints = []
    modinputs_checkpointed = set()
    for name, modinput in modinputs.items():
        name_ = extract_name(name)
        if name_ not in modinputs_checkpointed:
            checkpoint = get_checkpoints(session_key, name_)
            checkpoints.append(checkpoint)
            modinputs_checkpointed.update(checkpoint["modinputs"])

    return checkpoints


def _get_accounts(session_key, modinputs):
    accounts = set(modinput["account"] for modinput in modinputs.values())

    return {
        account["name"]: {
            k: v
            for k, v in account["content"].items()
            if k not in CONFIGURATION_PARAMS_TO_SKIP
        }
        for account in get_modinputs(session_key, "azureaccount")["entry"]
        if account["name"] in accounts
    }


def _get_modinputs(session_key, modinput_type):
    return {
        f'{modinput_type}://{entry["name"]}': {
            k: v
            for k, v in entry["content"].items()
            if k not in CONFIGURATION_PARAMS_TO_SKIP
        }
        for entry in get_modinputs(session_key, modinput_type)["entry"]
    }


def _get_storage_accounts(session_key, modinputs):
    storage_accounts = set(
        modinput["storage_account"]
        for modinput in modinputs.values()
        if "storage_account" in modinput
    )
    return {
        account["name"]: {
            k: v
            for k, v in account["content"].items()
            if k not in CONFIGURATION_PARAMS_TO_SKIP
        }
        for account in get_modinputs(session_key, "storageaccount")["entry"]
        if account["name"] in storage_accounts
    }


class ExportEndpoint(PersistentServerConnectionApplication):
    def __init__(self, _command_line, _command_arg):
        super(PersistentServerConnectionApplication, self).__init__()

    def handle(self, in_string):
        params = json.loads(in_string)
        session_key = params["session"]["authtoken"]

        server_info = ServerInfo(session_key)

        if not server_info.is_cloud_instance():
            return {
                "payload": {"ERROR": "Endpoint works only on Splunk Cloud instances"},
                "status": 404,
            }

        query_params = flatten_query_params(params["query"])
        skip_checkpoint_data = is_true(query_params.get("skip_checkpoint_data", False))

        try:
            result = {}

            modinputs = _get_modinputs(session_key, "mscs_i2_azure_event_hub")
            result["modinputs"] = {
                k: v
                for k, v in modinputs.items()
                if skip_checkpoint_data or is_true(v.get("export_status"))
            }
            modinputs = result["modinputs"]

            result["configuration"] = {
                "accounts": _get_accounts(session_key, modinputs),
                "storage_accounts": _get_storage_accounts(session_key, modinputs),
            }

            checkpoints = []
            if not skip_checkpoint_data:
                try:
                    checkpoints = _get_checkpoints(session_key, modinputs)
                except Exception:
                    checkpoints = _get_checkpoints(session_key, modinputs)

            result["checkpoints"] = checkpoints

            final_result = export_groups(result, not skip_checkpoint_data)
            final_result["timestamp"] = str(
                datetime.now(timezone.utc).replace(tzinfo=None)
            )
            final_result["server_name"] = str(server_info.server_name)

            return {
                "payload": json.dumps(final_result, ensure_ascii=False),
                "status": 200,
            }
        except Exception as e:
            return {
                "payload": {"ERROR": str(e)},
                "status": 400,
            }

    def handleStream(self, handle, in_string):
        raise NotImplementedError("PersistentServerConnectionApplication.handleStream")

    def done(self):
        pass


def extract_name(stanza_name):
    return stanza_name.split("://")[-1]
