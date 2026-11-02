#!/usr/bin/python
#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
import import_declare_test
import logging
from splunktaucclib.rest_handler import (
    admin_external,
    util,
)
from rest_handlers.mscs_storage_blob import (
    endpoint,
    StorageBlobInputHandler,
)

util.remove_http_proxy_env_vars()

if __name__ == "__main__":
    logging.getLogger().addHandler(logging.NullHandler())
    admin_external.handle(
        endpoint,
        handler=StorageBlobInputHandler,
    )
