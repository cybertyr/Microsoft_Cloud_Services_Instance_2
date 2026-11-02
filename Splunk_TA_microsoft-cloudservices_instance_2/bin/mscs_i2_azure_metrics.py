#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
import import_declare_test
import sys

from modular_inputs.scripts.mscs_azure_metrics import MscsAzureMetrics

if __name__ == "__main__":
    exit_code = MscsAzureMetrics().run(sys.argv)
    sys.exit(exit_code)
