##
## SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
## SPDX-License-Identifier: LicenseRef-Splunk-8-2021
##
##

[mscs_i2_stop_azure_vm]
python.version = python3
python.required = 3.9, 3.13
param.account = <string> the account stanza name in mscs_azure_accounts.conf
param.resource_group = <string> query the resources belong to the resource group
param.vm_name = <string> query the management events belong to the azure virtual machine
param.subscription_id = <string> query the management events belong to the subscription
