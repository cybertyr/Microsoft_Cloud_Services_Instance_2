##
## SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
## SPDX-License-Identifier: LicenseRef-Splunk-8-2021
##
##

[<name>]
description = <string> description of the input type
account = <string> the account stanza name in mscs_azure_accounts.conf
subscription_id = <string> query the management events belong to the subscription
start_time = <string>  the time to start querying from management events api
interval = <integer> the interval for the input, in seconds
index = <string> the index of the fetched data
audit_help_link = <string> URL for the link on which you want to redirect
