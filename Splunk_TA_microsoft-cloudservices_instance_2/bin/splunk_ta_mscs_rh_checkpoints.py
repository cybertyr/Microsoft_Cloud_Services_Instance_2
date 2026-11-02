#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
from splunk.persistconn.application import PersistentServerConnectionApplication

import sys
import json
from os import path, environ, getcwd

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

import modular_inputs.checkpoints.mscs_i2_azure_event_hub as eventhub_checkpoint
from rest_api_interface import flatten_query_params


class CheckpointsEndpoint(PersistentServerConnectionApplication):
    def __init__(self, _command_line, _command_arg):
        super(PersistentServerConnectionApplication, self).__init__()

    def handle(self, in_string):
        try:
            parameters = json.loads(in_string)
            session_key = parameters["session"]["authtoken"]
            query_parameters = flatten_query_params(parameters["query"])
            modinput_name = query_parameters.get("modinput_name")
            modinput_type = query_parameters.get("modinput_type")

            if modinput_type != "mscs_i2_azure_event_hub":
                return {
                    "payload": {
                        "ERROR": "Only mscs_i2_azure_event_hub modinput_type is supported"
                    },
                    "status": 400,
                }

            checkpoint = eventhub_checkpoint.export_checkpoint(
                session_key, modinput_name
            )

            return {"payload": checkpoint, "status": 200}
        except Exception as e:
            return {"payload": str(e), "status": 500}

    def handleStream(self, handle, in_string):
        raise NotImplementedError("PersistentServerConnectionApplication.handleStream")

    def done(self):
        pass
