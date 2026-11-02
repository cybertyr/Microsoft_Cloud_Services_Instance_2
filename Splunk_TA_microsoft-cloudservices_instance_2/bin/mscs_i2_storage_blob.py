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
from mscs_storage_blob_data_client import StorageBlobDataClient
from mscs_util import get_schema_file_path
import mscs_consts


def ta_run():
    schema_file_path = get_schema_file_path("mscs_schema.storage_blob_config.json")
    ta_input.main(
        collector_cls=StorageBlobDataClient,
        schema_file_path=schema_file_path,
        log_suffix="storage_blob",
        schema_para_list=(
            mscs_consts.DESCRIPTION,
            mscs_consts.ACCOUNT,
            mscs_consts.CONTAINER_NAME,
            mscs_consts.PREFIX,
            mscs_consts.BLOB_LIST,
            mscs_consts.COLLECTION_INTERVAL,
            mscs_consts.EXCLUDE_BLOB_LIST,
            mscs_consts.BLOB_MODE,
            mscs_consts.BLOB_COMPRESSION,
            mscs_consts.DONT_REUPLOAD_BLOB_SAME_SIZE,
            mscs_consts.IS_MIGRATED,
            mscs_consts.DECODING,
            mscs_consts.PARSE_CSV_WITH_DELIMITER,
            mscs_consts.PARSE_CSV_WITH_HEADER,
            mscs_consts.LOG_TYPE,
            mscs_consts.GUIDS,
            mscs_consts.APPLICATION_INSIGHTS,
            mscs_consts.BLOB_INPUT_HELP_LINK,
            mscs_consts.READ_TIMEOUT,
            mscs_consts.WORKER_THREADS_NUM,
            mscs_consts.GET_BLOB_BATCH_SIZE,
            mscs_consts.AGENT,
        ),
        single_instance=False,
    )


if __name__ == "__main__":
    ta_run()
