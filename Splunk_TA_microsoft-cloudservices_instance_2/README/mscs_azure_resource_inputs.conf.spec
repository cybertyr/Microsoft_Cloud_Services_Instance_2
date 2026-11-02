##
## SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
## SPDX-License-Identifier: LicenseRef-Splunk-8-2021
##
##

[<name>]
description = <string> description of the input type
account = <string> the account stanza name in mscs_azure_accounts.conf
subscription_id = <string> query the management events belong to the subscription
resource_type = <string> the api stanza name in mscs_api_settings.conf
resource_group_list = <string> query the resources belong to the resource group list
network_watcher_name = <string> Network Watchers provide access to topology data.
network_watcher_resource_group = <string> Specify the Resource Group containing the Network Watcher.
target_resource_group = <string> Specify the Resource Group to enumerate topology. This Resource Group should be in the same region as the Network Watcher.
resource_graph_query = <string> query fetch the resources
interval = <integer> the interval for the input, in seconds
index = <string> the index of the fetched data
resource_help_link = <string> URL for the link on which you want to redirect
max_attempts = <integer> Maximum number of attempts to retry throttled (HTTP 429) or unavailable (HTTP 503) requests. Overrides the global setting.
