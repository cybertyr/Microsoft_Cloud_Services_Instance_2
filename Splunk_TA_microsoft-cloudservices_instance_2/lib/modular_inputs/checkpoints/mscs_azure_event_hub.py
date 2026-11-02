#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
from typing import List, Dict
from os import path, environ, getcwd

from azure.core.exceptions import ResourceNotFoundError
from modular_inputs.mscs_azure_event_hub import SharedLocalCheckpoint
from mscs_storage_service import (
    _create_blob_checkpoint_store_service,
)
from rest_api_interface import (
    get_modinputs,
    get_storage_accounts,
    get_proxy_info_from_endpoint,
)
from splunk_ta_mscs.models import AzureStorageAccountConfig

from solnlib.utils import is_false, is_true


def get_unique_id(modinput: Dict[str, str]) -> str:
    unique_id = (
        f"{modinput['event_hub_namespace']}-"
        f"{modinput['event_hub_name']}-"
        f"{modinput['consumer_group']}"
    )

    if is_true(modinput.get("blob_checkpoint_enabled", "0")):
        unique_id = unique_id + "_blob"

    return unique_id


def _export_blob_checkpoint(session_key: str, modinput: dict) -> (str, List):
    accounts = get_storage_accounts(session_key)
    storage_account = AzureStorageAccountConfig.from_dict(
        accounts[modinput["storage_account"]]
    )
    proxies = get_proxy_info_from_endpoint(session_key)

    try:
        blob_service_checkpoint = _create_blob_checkpoint_store_service(
            storage_account=storage_account,
            container_name=modinput["container_name"],
            proxies=proxies.proxy_dict,
        )

        checkpoints = blob_service_checkpoint.list_checkpoints(
            fully_qualified_namespace=modinput["event_hub_namespace"],
            eventhub_name=modinput["event_hub_name"],
            consumer_group=modinput["consumer_group"],
        )
    except ResourceNotFoundError:
        return []

    result = [
        {
            "partition_id": checkpoint["partition_id"],
            "offset": checkpoint["offset"],
            "sequence_number": checkpoint["sequence_number"],
        }
        for checkpoint in checkpoints
    ]

    return result


def _export_local_checkpoint(modinput) -> (str, List):
    unique_checkpoint_id = get_unique_id(modinput)

    file_path = path.join(
        environ.get("SPLUNK_HOME", getcwd()),
        "var",
        "lib",
        "splunk",
        "modinputs",
        "mscs_i2_azure_event_hub",
        f"{unique_checkpoint_id}.v1.ckpt",
    )

    checkpoint = SharedLocalCheckpoint(file_path)
    result = []
    for i in range(checkpoint._number_of_partition):
        index = checkpoint._get_page_index(i, 1)
        checkpoint_ = checkpoint._read_record(index)
        if not checkpoint_:
            continue
        _, _, offset, sequence_number = checkpoint_
        result.append(
            {
                "partition_id": str(i),
                "offset": offset,
                "sequence_number": sequence_number,
            }
        )

    return result


def _find_other_modinputs_using_the_same_checkpoint(session_key, unique_checkpoint_id):
    result = []
    modinputs = get_modinputs(session_key, "mscs_i2_azure_event_hub")
    for modinput in modinputs["entry"]:
        cpt_id = get_unique_id(modinput["content"])
        if cpt_id == unique_checkpoint_id:
            result.append(modinput)

    return result


def export_checkpoint(session_key, modinput_name: str) -> Dict:
    skip_checkpoint_data = False

    try:
        modinput = get_modinputs(session_key, "mscs_i2_azure_event_hub", modinput_name)[
            "entry"
        ][0]["content"]
    except Exception:
        return {
            "type": "",
            "unique_identifier": "",
            "modinputs": [],
            "data": [],
            "errors": [f"Modinput {modinput_name} doesn't exist"],
        }

    unique_id = get_unique_id(modinput)
    other_modinputs = _find_other_modinputs_using_the_same_checkpoint(
        session_key, unique_id
    )

    disabled_modinputs = [
        modinput_
        for modinput_ in other_modinputs
        if not modinput_["content"]["disabled"]
    ]

    errors = []
    if disabled_modinputs:
        skip_checkpoint_data = True
        errors = (
            f"Cannot get checkpoint data while modinputs are not disabled: "
            f"{', '.join(modinput_['name'] for modinput_ in disabled_modinputs)}"
        )

    data = []
    if is_false(modinput.get("blob_checkpoint_enabled", "0")):
        if not skip_checkpoint_data:
            data = _export_local_checkpoint(modinput)
        type_ = "LocalCheckpoint"
    else:
        if not skip_checkpoint_data:
            data = _export_blob_checkpoint(session_key, modinput)
        type_ = "BlobCheckpoint"

    return {
        "type": type_,
        "unique_identifier": unique_id,
        "modinputs": sorted(modinput_["name"] for modinput_ in other_modinputs),
        "data": data,
        "errors": errors,
    }
