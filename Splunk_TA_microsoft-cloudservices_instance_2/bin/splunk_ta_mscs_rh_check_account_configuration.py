#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
"""
This module will be used to validate the if the account is valid or not
"""
import import_declare_test
import splunk.admin as admin
from rest_handlers.check_account_configuration import (
    AccountCheckConfigurationHandler,
)

if __name__ == "__main__":
    admin.init(AccountCheckConfigurationHandler, admin.CONTEXT_APP_AND_USER)
