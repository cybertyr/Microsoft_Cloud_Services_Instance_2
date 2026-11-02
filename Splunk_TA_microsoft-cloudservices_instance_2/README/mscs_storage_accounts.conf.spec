##
## SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
## SPDX-License-Identifier: LicenseRef-Splunk-8-2021
##
##

[<account_stanza_name>]
account_name = <string> the name for the storage account
account_secret = <string> the access key or shared access signature token for the storage account
account_secret_type = <integer> should be 0 or 1 or 2. 1 means the account_secret stands for access key, 2 means the account_secret stands for shared access signature, 0 means user doesn't afford the account_secret.
account_class_type = <integer> should be 1 or 2. 1 for azure public cloud, 2 for azure government cloud
