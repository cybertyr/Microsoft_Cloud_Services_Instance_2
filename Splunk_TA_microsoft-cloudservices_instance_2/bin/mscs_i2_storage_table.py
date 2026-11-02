#!/usr/bin/python
#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
"""
This is the main entry point for My TA
"""
import import_declare_test
import splunktaucclib.data_collection.ta_mod_input as ta_input
from mscs_storage_table_data_client import StorageTableDataClient as collector_cls
from mscs_util import get_schema_file_path


def ta_run():
    schema_file_path = get_schema_file_path("mscs_schema.storage_table_config.json")
    ta_input.main(
        collector_cls,
        schema_file_path,
        "storage_table",
        schema_para_list=(
            "description",
            "account",
            "start_time",
            "table_list",
            "collection_interval",
            "storage_input_help_link",
            "storage_virtual_metrics_input_help_link",
            "query_entities_page_size",
            "event_cnt_per_item",
            "query_end_time_offset",
            "agent",
        ),
        single_instance=False,
    )


if __name__ == "__main__":
    ta_run()
