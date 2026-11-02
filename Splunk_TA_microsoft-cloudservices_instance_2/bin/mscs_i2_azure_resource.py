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
from mscs_azure_resource_data_collector import do_job_one_time as collector_cls
from mscs_util import get_schema_file_path, setup_log_level


if __name__ == "__main__":
    setup_log_level()
    ta_input.main(
        collector_cls,
        get_schema_file_path("mscs_schema.azure_resource_config.json"),
        "azure_resource",
        schema_para_list=("description",),
    )
