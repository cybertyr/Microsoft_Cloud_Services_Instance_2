"""
Custom REST Endpoint for enumerating Indexes for Azure Metrics Input.
"""

#
# SPDX-FileCopyrightText: 2021 Splunk, Inc. <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
import import_declare_test
from splunk import admin
from rest_handlers.index import ConfigHandler

if __name__ == "__main__":
    admin.init(ConfigHandler, admin.CONTEXT_NONE)
