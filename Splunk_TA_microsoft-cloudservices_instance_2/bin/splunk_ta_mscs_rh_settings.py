#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
import import_declare_test
import logging
from splunktaucclib.rest_handler import admin_external
from rest_handlers.settings import endpoint, CustomMSCSSettingsHandler
from splunktaucclib.rest_handler import util

util.remove_http_proxy_env_vars()

if __name__ == "__main__":
    logging.getLogger().addHandler(logging.NullHandler())
    admin_external.handle(
        endpoint,
        handler=CustomMSCSSettingsHandler,
    )
