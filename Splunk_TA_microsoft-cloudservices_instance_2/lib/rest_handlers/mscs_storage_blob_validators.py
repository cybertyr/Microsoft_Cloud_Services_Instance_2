#!/usr/bin/python
#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
from solnlib import log
from splunktaucclib.rest_handler.endpoint import (
    validator,
)
import mscs_consts


class CompressionTypeValidator(validator.Validator):
    def __init__(self):

        super(CompressionTypeValidator, self).__init__()
        self.blob_compression = None
        self.blob_mode = None

    def validate(self, _, data):
        logger = log.Logs().get_logger(
            "splunk_ta_microsoft-cloudservices_instance_2_storage_blob_validation"
        )

        logger.info("Verifying compression type for MSCS Azure storage blob input")

        self.blob_mode = data.get(mscs_consts.BLOB_MODE)
        self.blob_compression = data.get(mscs_consts.BLOB_COMPRESSION)

        if not self.blob_compression and not self.blob_mode:
            return True

        if self.blob_compression not in mscs_consts.BLOB_COMPRESSION_LIST:
            supported_values = ", ".join(
                [f'"{v}"' for v in mscs_consts.BLOB_COMPRESSION_LIST]
            )
            self.put_msg(
                f'Unsupported blob compression type specified "{self.blob_compression}", supported values are: {supported_values}'
            )
            return False

        if self.blob_mode not in mscs_consts.BLOB_MODE_LIST:
            supported_values = ", ".join([f'"{v}"' for v in mscs_consts.BLOB_MODE_LIST])
            self.put_msg(
                f'Unsupported blob mode is specified "{self.blob_mode}" , supported values are {supported_values}'
            )
            return False

        if (
            self.blob_mode != mscs_consts.BLOB_MODE_RANDOM
            and self.blob_compression != mscs_consts.BLOB_NOT_COMPRESSED
        ):
            self.put_msg(
                f"Compression is only supported for {mscs_consts.BLOB_MODE_RANDOM} blob mode."
            )
            return False

        logger.info(
            "Compression type for MSCS Azure storage blob input validated successfully"
        )
        return True
