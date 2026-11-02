#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#

from collections import defaultdict

from solnlib.utils import is_true


def extract_name(stanza_name):
    return stanza_name.split("://")[-1]


def _get_common_values(data_):
    def _check_if_all(group, param_name, param):
        for (_, modinput) in group:
            if not (param_name in modinput and modinput[param_name] == param):
                return False

        return True

    common_params = {}
    final_common_params = {}
    for unique_group_id, group in data_.items():
        common_params = group[0][1]
        final_common_params[unique_group_id] = {}
        for param_name, param in common_params.items():
            if _check_if_all(group, param_name, param):
                final_common_params[unique_group_id][param_name] = param

    return final_common_params


def _map_accounts(final_data, data):
    for entry in final_data["modinputs"]:
        if "account" in entry["config"]:
            entry["config"]["account"] = data["configuration"]["accounts"].get(
                entry["config"]["account"], {}
            )
        if "storage_account" in entry["config"]:
            entry["config"]["storage_account"] = data["configuration"][
                "storage_accounts"
            ].get(entry["config"]["storage_account"], {})

        for input_ in entry["meta"]["inputs"]:
            input_config = input_["unique_config"]

            if "account" in input_config:
                input_config["account"] = data["configuration"]["accounts"].get(
                    input_config["account"], {}
                )
            if "storage_account" in input_config:
                input_config["storage_account"] = data["configuration"][
                    "storage_accounts"
                ].get(input_config["storage_account"], {})


def _check_errors(final_data):
    for entry in final_data["modinputs"]:
        if "disabled" in entry["config"]:
            if not entry["config"].get("disabled"):
                entry["errors"].append(
                    {
                        "message": f'To export checkpoints please disable modinputs: {", ".join(input_["name"] for input_ in entry["meta"]["inputs"])}',
                        "type": "ME001",
                    }
                )
        else:
            not_disabled = [
                input["name"]
                for input in entry["meta"]["inputs"]
                if not input["unique_config"].get("disabled")
            ]
            if not_disabled:
                entry["errors"].append(
                    {
                        "message": f"To export checkpoints please disable modinputs: {', '.join(not_disabled)}",
                        "type": "ME001",
                    }
                )

        if "sourcetype" not in entry["config"]:
            diff_sourcetype = defaultdict(list)

            for input in entry["meta"]["inputs"]:
                diff_sourcetype[input["unique_config"]["sourcetype"]].append(
                    input["name"]
                )

            entry["errors"].append(
                {
                    "message": f"Different sourcetypes used for modinputs connected to the same EventHub "
                    f"{' and '.join(f'[sourcetype={k}] used in modinputs {v}' for k,v in diff_sourcetype.items())}",
                    "type": "ME002",
                }
            )

        if "account" not in entry["config"]:
            diff_account = defaultdict(list)

            for input in entry["meta"]["inputs"]:
                diff_account[
                    (
                        input["unique_config"]["account"]["client_id"],
                        input["unique_config"]["account"]["tenant_id"],
                    )
                ].append(input["name"])

            entry["errors"].append(
                {
                    "message": f"Different accounts used for modinputs connected to the same EventHub "
                    f"{' and '.join(f'[client_id={k[0]}, tenant_id={k[1]}] used in modinputs {v}' for k,v in diff_account.items())}",
                    "type": "ME003",
                }
            )


def _merge_accounts(final_data):
    for entry in final_data["modinputs"]:
        account_guard = entry["meta"]["inputs"][0]["unique_config"].get("account")
        storage_account_guard = entry["meta"]["inputs"][0]["unique_config"].get(
            "storage_account"
        )

        if (
            "account" not in entry["meta"]["inputs"]
            and account_guard is not None
            and all(
                account_guard == input_["unique_config"].get("account")
                for input_ in entry["meta"]["inputs"]
            )
        ):
            for input_ in entry["meta"]["inputs"]:
                if "account" in input_["unique_config"]:
                    del input_["unique_config"]["account"]
            entry["config"]["account"] = account_guard

        if (
            "storage_account" not in entry["meta"]["inputs"]
            and storage_account_guard is not None
            and all(
                storage_account_guard == input_["unique_config"].get("storage_account")
                for input_ in entry["meta"]["inputs"]
            )
        ):
            for input_ in entry["meta"]["inputs"]:
                if "storage_account" in input_["unique_config"]:
                    del input_["unique_config"]["storage_account"]
            entry["config"]["storage_account"] = storage_account_guard


def _delete_not_in_export(final_data):
    final_data["modinputs"] = list(
        filter(
            lambda entry: is_true(entry["config"].get("export_status")),
            final_data["modinputs"],
        )
    )


def export_groups(data, only_in_export=False):
    EVENTHUB_GROUP_ID = lambda modinput: (
        modinput["event_hub_namespace"],
        modinput["event_hub_name"],
        modinput["consumer_group"],
    )

    # Group by ID
    data_new = defaultdict(list)
    for name, modinput in data["modinputs"].items():
        unique_group_id = EVENTHUB_GROUP_ID(modinput)
        data_new[unique_group_id].append((name, modinput))

    common_values = _get_common_values(data_new)

    final_data = {
        "modinputs": [
            {
                "meta": {
                    "type": "mscs_i2_azure_event_hub",
                    "inputs": sorted(
                        (
                            {
                                "name": extract_name(name),
                                "unique_config": {
                                    k: v
                                    for k, v in modinput.items()
                                    if k not in common_values[unique_id]
                                },
                            }
                            for name, modinput in group
                        ),
                        key=lambda x: x["name"],
                    ),
                    "unique_identifier": str(unique_id),
                },
                "config": common_values[unique_id],
                "checkpoints": [
                    x
                    for x in data["checkpoints"]
                    if any(extract_name(name) in x["modinputs"] for name, _ in group)
                ],
                "errors": [],
            }
            for unique_id, group in data_new.items()
        ]
    }

    if only_in_export:
        _delete_not_in_export(final_data)
    _map_accounts(final_data, data)
    _merge_accounts(final_data)
    _check_errors(final_data)

    return final_data
