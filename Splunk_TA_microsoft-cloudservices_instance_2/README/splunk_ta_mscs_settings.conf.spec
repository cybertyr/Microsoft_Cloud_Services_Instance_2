##
## SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
## SPDX-License-Identifier: LicenseRef-Splunk-8-2021
##
##

[proxy]
proxy_enabled = <boolean> bool value indicate if the proxy is enabled or not
proxy_rdns = <string> DNS resolution of the proxy
proxy_type = <string> type of proxy, like http, http_no_tunnel, socks4, socks5
proxy_password = <string> password of proxy account
proxy_port = <integer> port of proxy
proxy_url = <string> proxy host
proxy_username = <string> user name of proxy account

[logging]
agent = <string> the loglevel of TA

[performance_tuning_settings]
worker_threads_num = <integer> Amount of workers responsible for collecting the data from individual blobs simultaneously. It's used for tuning the parallel processing of Storage Blobs.
query_entities_page_size = <integer> Amount of entities to be retrieved per page when querying entities from an Storage Table. It helps in controlling the amount of data fetched in each query operation.
event_cnt_per_item = <integer> Amount of entities to be processed and converted into events in a single batch. This attribute helps in controlling the batch size for event processing for Storage Table.
query_end_time_offset = <integer> The offset in seconds from the current time to determine the end time for querying entities. This attribute helps in setting a time window for the data collection process, ensuring that the queries do not include data that is too recent and might still be in flux. It's used in Storage Table and Azure Audit.
get_blob_batch_size = <integer> Amount of bytes to be downloaded in a batch for append blobs. This attribute helps in controlling the size of each download operation. It's used in Storage Blobs.
http_timeout = <integer> Http timeout
skip_files_monitoring_for_restart = <boolean> When enabled, modular input doesn't monitor config file changes to automatically restart itself and needs to be re-enabled manually. It's used in Event Hub.
event_hub_watchdog_enabled = <boolean> Enabled by default. Event Hub inputs compare Azure partition sequence numbers with local ingestion progress and recreate stalled consumers. Set to 0 to disable globally.
load_balancing_strategy = <string> Strategy to claim and balance the partition ownership
max_attempts = <integer> Maximum number of attempts to retry throttled (HTTP 429) or unavailable (HTTP 503) requests. Used by Azure Resource and Azure Audit inputs.
