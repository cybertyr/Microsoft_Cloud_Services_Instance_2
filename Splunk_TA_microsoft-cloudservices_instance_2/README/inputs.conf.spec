##
## SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
## SPDX-License-Identifier: LicenseRef-Splunk-8-2021
##
##
[mscs_i2_storage_blob]
python.version = {python3}
python.required = 3.9, 3.13

[mscs_i2_storage_blob://<name>]
description = <string> description of the input type
account = <string> the account stanza name in mscs_storage_accounts.conf
container_name = <string> the container name under the storage account
prefix = <string> input will only collect the data from the blobs whose names begin with specified prefix
blob_list = <string>  the blob list from which the data should be collected
collection_interval = <integer> the interval for the input, in seconds
exclude_blob_list = <string> the blob list from which the data should not be collected
blob_mode = <string> blob mode of the api (default is random)
is_migrated = [0|1] Whether checkpoint has been migrated or not.
decoding = <string> the character set of the blobs. e.g UTF-8, UTF-32, etc.
log_type = <string> Filters the results to return only blobs whose names begin with the specified prefix.
guids = <string> List the individual comma separated GUIDs
application_insights = <checkbox> Blob collection will be changed if this is set to true
index = <string> the index of the fetched data
sourcetype = <string> the sourcetype of the fetched data
blob_input_help_link = <string> URL for the link on which you want to redirect
read_timeout = <integer> the read timeout parameter that specifies the maximum amount of time to wait for a response from the Azure Storage service when reading data.
blob_compression= [not_compressed|extension_based|gzip] supported blob compression type if any, extension_based if to be detected from blob name (not available in UI), otherwise not_compressed. Compression with 'append' blob mode is not supported and should be set to not_compressed. Currently supported compression types: gzip; supported blob name extensions: .gz; extension to compression type mappings:  .gz -> gzip
parse_csv_with_header = [0|1] Enable parsing of CSV data with header
parse_csv_with_delimiter = <string> Enable parsing of CSV data by chosen delimiter
dont_reupload_blob_same_size = [0|1] Disable this option to process blobs which were reuploaded but their size didn't changed.
worker_threads_num = <integer> Amount of workers responsible for collecting the data from individual blobs simultaneously. It's used for tuning the parallel processing of Storage Blobs.
get_blob_batch_size = <integer> Amount of bytes to be downloaded in a batch for append blobs. This attribute helps in controlling the size of each download operation. It's used in Storage Blobs.
agent = <string> the log level of TA input

[mscs_i2_storage_table]
python.version = {python3}
python.required = 3.9, 3.13

[mscs_i2_storage_table://<name>]
description = <string> description of the input type
account = <string> the account stanza name in mscs_storage_accounts.conf
storage_table_type = <string> storage_table or virtual_machine_metrics
table_list = <string> the names of the tables to query
start_time = <string> the time to start querying from storage api
collection_interval = <integer> the interval for the input, in seconds
index = <string> the index of the fetched data
sourcetype = <string> the sourcetype of the fetched data
storage_input_help_link = <string> URL for the link on which you want to redirect
storage_virtual_metrics_input_help_link = <string> URL for the link on which you want to redirect
query_entities_page_size = <integer> Amount of entities to be retrieved per page when querying entities from an Storage Table. It helps in controlling the amount of data fetched in each query operation.
event_cnt_per_item = <integer> Amount of entities to be processed and converted into events in a single batch. This attribute helps in controlling the batch size for event processing for Storage Table.
query_end_time_offset = <integer> The offset in seconds from the current time to determine the end time for querying entities. This attribute helps in setting a time window for the data collection process, ensuring that the queries do not include data that is too recent and might still be in flux. It's used in Storage Table and Azure Audit.
agent = <string> the log level of TA input

[mscs_i2_azure_resource]
python.version = {python3}
python.required = 3.9, 3.13

[mscs_i2_azure_resource://<name>]
description = <string> description of the input type

[mscs_i2_azure_audit]
python.version = {python3}
python.required = 3.9, 3.13

[mscs_i2_azure_audit://<name>]
description = <string> description of the input type

[mscs_i2_azure_event_hub]
python.version = {python3}
python.required = 3.9, 3.13

[mscs_i2_azure_event_hub://<name>]
account = <string> the account stanza name in mscs_azure_accounts.conf
event_hub_namespace = <string> The Azure Event Hub Namespace (FQDN)
event_hub_name = <string> The Azure Event Hub Name
consumer_group = <string> The Azure Event Hub Consume Group
blob_checkpoint_enabled = <boolean> bool value indicates if storage blob checkpointing is enabled for Eventhubs
storage_account = <string> Storage Account
container_name = <string> Storage Blob Container name for EventHub checkpoint
max_wait_time = <integer> The maximum interval in seconds that the event processor will wait before processing
max_batch_size = <integer> The maximum number of events that would be retrieved in one batch
event_format_flags = <integer> The bitwise flags that determines the format of output events
use_amqp_over_websocket = <boolean> The switch that allow using AMQP over WebSocket
ensure_ascii = <boolean> The switch that allows enforce ASCII encoding of ingested events
export_status = <boolean> The export status of modular input to Splunk Data Manager
sourcetype = <string> the sourcetype of the fetched data
force_amqp_over_proxy = <boolean> The switch that allows to override the proxy with AMQP configuration

[mscs_i2_azure_consumption]
python.version = {python3}
python.required = 3.9, 3.13

[mscs_i2_azure_consumption://<name>]
account = <string> The account stanza name in mscs_azure_accounts.conf
subscription_id = <string> Query the consumption data belong to the subscription
data_type = <string>The type of data to be collected.Supported data types are (i)Reservation Recommendation (ii)Usage Details
query_days = <integer> Specify the maximum number of days to query each interval.
start_date = <string> Date from which data collection will be started.

[mscs_i2_azure_kql]
python.version = {python3}
python.required = 3.9, 3.13

[mscs_i2_azure_kql://<name>]
index = <string> the index of the fetched data
sourcetype = <string> the sourcetype of the fetched data
account = <string> the account stanza name in mscs_azure_accounts.conf
workspace_id = <string> the id of workspace with which kql query will run
kql_query = <string> kql query to run on given workspace
index_stats = <boolean> if checked, the input will index statistics about the KQL query. The term ':stats' will be appended to the specified sourcetype for the statistical data
index_empty_values = <boolean> if checked, the input will also index event's fields having empty values

[mscs_i2_azure_metrics]
python.version = {python3}
python.required = 3.9, 3.13

[mscs_i2_azure_metrics://<name>]
account = <string> the account stanza name in mscs_azure_accounts.conf
subscription_id = <string> query the metrics data belong to the subscription
namespaces = <string> comma-separated list of metric namespaces to query
metric_statistics = <string> the type of statistic to gather
preferred_time_aggregation = <string> the preferred aggregation type
metric_index_flag = <string> flag to use metric index or not
number_of_threads = <integer> the number of threads used to download metric data in parallel
