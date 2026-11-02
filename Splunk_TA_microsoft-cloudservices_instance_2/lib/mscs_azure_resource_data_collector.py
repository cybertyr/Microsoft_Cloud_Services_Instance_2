#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
import json
import re

from azure.mgmt.resource import ResourceManagementClient

import mscs_api_error as mae
import mscs_azure_base_data_collector as mabdc
import mscs_consts
import splunktaucclib.data_collection.ta_data_client as dc

from collections import defaultdict
from solnlib.observability import StanzaObservabilityRecorder


_RESOURCE_NOT_FOUND = "ResourceNotFound"
_RESOURCE_GROUP_NOT_FOUND = "ResourceGroupNotFound"


@dc.client_adapter
def do_job_one_time(all_conf_contents, task_config, ckpt):
    collector = AzureResourceDataCollector(all_conf_contents, task_config)
    return collector.collect_data()


class AzureResourceType:
    VIRTUAL_MACHINE = "virtual_machine"
    PUBLIC_IP_ADDRESS = "public_ip_address"
    TOPOLOGY = "topology"
    SUBSCRIPTIONS = "subscriptions"
    RESOURCE_GRAPH = "resource_graph"


class AzureResourceSourceType:
    RESOURCE_GRAPH = "mscs:resource:resourceGraph"


class AzureResourceSource:
    RESOURCE_GRAPH_COMMON_SOURCE_STR = "mscs_resource_graph:tenant_id"


class AzureResourceIdPattern:
    VNET_PATTERN = '\/subscriptions\/(?P<subscriptionId>[^\/]+)\/resourceGroups\/(?P<resourceGroup>[^\/]+)\/providers\/Microsoft\.Network\/virtualNetworks\/(?P<vnetName>[^"]+)'
    NIC_PATTERN = '\/subscriptions\/(?P<subscriptionId>[^\/]+)\/resourceGroups\/(?P<resourceGroup>[^\/]+)\/providers\/Microsoft\.Network\/networkInterfaces/(?P<nicName>[^"]+)'
    SUBNET_PATTERN = '\/subscriptions\/(?P<subscriptionId>[^\/]+)\/resourceGroups\/(?P<resourceGroup>[^\/]+)\/providers\/Microsoft\.Network\/virtualNetworks\/(?P<vnetName>[^\/]+)\/(?P<resourceType>[^\/]+)\/(?P<subnetName>[^"]+)'
    VM_PATTERN = '\/subscriptions\/(?P<subscriptionId>[^\/]+)\/resourceGroups\/(?P<resourceGroup>[^\/]+)\/providers\/Microsoft\.Compute\/virtualMachines\/(?P<vmName>[^"]+)'
    NSG_PATTERN = '\/subscriptions\/(?P<subscriptionId>[^\/]+)\/resourceGroups\/(?P<resourceGroup>[^\/]+)\/providers\/Microsoft\.Network\/networkSecurityGroups\/(?P<securityGroupName>[^"]+)$'


class AzureResourceDataCollector(mabdc.AzureBaseDataCollector):
    def __init__(self, all_conf_contents, task_config):
        super(AzureResourceDataCollector, self).__init__(all_conf_contents, task_config)
        self._resource_type = task_config[mscs_consts.RESOURCE_TYPE]
        self._resource_group_list_str = task_config.get(mscs_consts.RESOURCE_GROUP_LIST)
        self._index = task_config[mscs_consts.INDEX]
        self._resource_graph_query = task_config.get(mscs_consts.RESOURCE_GRAPH_QUERY)

        self._topology_params = {
            mscs_consts.NETWORK_WATCHER_NAME: task_config.get(
                mscs_consts.NETWORK_WATCHER_NAME
            ),
            mscs_consts.NETWORK_WATCHER_RESOURCE_GROUP: task_config.get(
                mscs_consts.NETWORK_WATCHER_RESOURCE_GROUP
            ),
            mscs_consts.TARGET_RESOURCE_GROUP: task_config.get(
                mscs_consts.TARGET_RESOURCE_GROUP
            ),
        }

        self._obs = StanzaObservabilityRecorder(
            "resource", self._logger, task_config[mscs_consts.STANZA_NAME]
        )

        #  needed for enabling multicloud
        #  https://learn.microsoft.com/en-us/azure/developer/python/sdk/azure-sdk-sovereign-domain
        self._credential_scopes = [self._manager_url + "/.default"]

        if self._subscription_id and self._resource_type != "subscriptions":
            self._resource_client = ResourceManagementClient(
                credential=self._credentials,
                subscription_id=self._subscription_id,
                proxies=self._proxies.proxy_dict,
                base_url=self._manager_url,
                credential_scopes=self._credential_scopes,
            )

        if self._resource_type == AzureResourceType.RESOURCE_GRAPH:
            self._parse_api_setting(mscs_consts.RESOURCE_GRAPH_API_TYPE)
            self._sourcetype = AzureResourceSourceType.RESOURCE_GRAPH
        else:
            self._parse_api_setting(self._resource_type)

    def collect_data(self):
        self._logger.info("Starting to collect data.")
        self._logger.info("The resource_type=%s", self._resource_type)

        try:
            if self._resource_type == AzureResourceType.SUBSCRIPTIONS:
                url = self._url.format(
                    api_version=self._api_version, base_host=self._manager_url
                )

                # Method to fetch the events
                yield from self._fetch_events_for_resource(url)
            elif self._resource_type == AzureResourceType.TOPOLOGY:
                yield from self._get_topology()
            elif self._resource_type == AzureResourceType.RESOURCE_GRAPH:
                yield from self._fetch_events_for_resource_graph(
                    self._subscription_id, self._resource_graph_query
                )
            else:
                resource_group_list = self._get_resource_group_list()

                self._logger.info("The resource_group_list=%s", resource_group_list)

                for resource_group in resource_group_list:

                    # In first request next_link will be none only
                    url = self._generate_url(
                        resource_group=resource_group, next_link=None
                    )

                    # Method to fetch the events
                    yield from self._fetch_events_for_resource(
                        url, resource_group=resource_group
                    )

            self._logger.info("Finishing collect data.")
        finally:
            self._obs.flush()

    def _parse_api_setting(self, api_stanza_name: str) -> dict:
        """API settings parser

        Args:
            api_stanza_name (str): Name of the API stanza

        Returns:
            dict: Returns dict of API settings
        """
        api_setting = super(AzureResourceDataCollector, self)._parse_api_setting(
            api_stanza_name
        )
        if self._resource_type == AzureResourceType.VIRTUAL_MACHINE:
            self._instance_view_url = api_setting[mscs_consts.INSTANCE_VIEW_URL]
        if self._resource_type == AzureResourceType.TOPOLOGY:
            self._network_watcher_url = api_setting[mscs_consts.NETWORK_WATCHER_URL]
        return api_setting

    def _get_logger_prefix(self):
        account_stanza_name = self._task_config[mscs_consts.ACCOUNT]
        pairs = [
            '{}="{}"'.format(
                mscs_consts.STANZA_NAME, self._task_config[mscs_consts.STANZA_NAME]
            ),
            '{}="{}"'.format(mscs_consts.ACCOUNT, account_stanza_name),
            '{}="{}"'.format(
                mscs_consts.RESOURCE_TYPE, self._task_config[mscs_consts.RESOURCE_TYPE]
            ),
        ]
        return "[{}]".format(" ".join(pairs))

    def _get_resource_group_list(self):
        if not self._resource_group_list_str:
            resource_groups = self._resource_client.resource_groups.list()
            return [group.name for group in resource_groups]
        else:
            return list(
                {
                    resource_group.strip().lower()
                    for resource_group in self._resource_group_list_str.split(",")
                    if len(resource_group.strip())
                }
            )

    def _get_resource_groups_by_location(self) -> list:
        """Fetch list of resource groups by location

        Returns:
            list: Return list of dict
        """
        resource_groups = self._resource_client.resource_groups.list()
        resource_groups_by_location = defaultdict(list)

        for group in resource_groups:
            key = group.location
            resource_groups_by_location[key].append(group.name)

        return resource_groups_by_location

    def _get_detailed_network_watcher_list(self) -> list:
        """Fetch network watchers

        Returns:
            list: Returns list of network watchers
        """
        url = self._network_watcher_url.format(
            subscription_id=self._subscription_id,
            api_version=self._api_version,
            base_host=self._manager_url,
        )
        results = []
        while True:
            result = self._perform_request(url)
            results.extend(result.get("value"))

            # check next_link if present, then assign it url variable
            next_link = result.get(self._NEXT_LINK)

            if not next_link:
                break
            else:
                url = next_link

        return results

    def _get_detailed_vm_info_list(self, resource_group, vm_info_lst):
        for vm_info in vm_info_lst:
            try:
                vm_name = vm_info["name"]
                url = self._generate_url(resource_group=resource_group, vm_name=vm_name)
                result = self._perform_request(url)
                vm_info["properties"]["instanceView"] = result
                tags = vm_info.get("tags")
                if not tags:
                    continue
                tag_lst = []
                for (tag_k, tag_v) in tags.items():
                    tag_lst.append(tag_k + " : " + tag_v)
                vm_info["tags"] = tag_lst
            except mae.APIError as e:
                if e.error_code == _RESOURCE_NOT_FOUND:
                    self._logger.warning(f'message="Resource not found: {e}"')
                else:
                    raise e
        return vm_info_lst

    def _generate_url(
        self, resource_group, vm_name=None, network_watcher_name=None, next_link=None
    ):
        if next_link:
            return next_link
        if vm_name:
            return self._instance_view_url.format(
                subscription_id=self._subscription_id,
                resource_group_name=resource_group,
                vm_name=vm_name,
                api_version=self._api_version,
                base_host=self._manager_url,
            )
        elif network_watcher_name:
            return self._url.format(
                subscription_id=self._subscription_id,
                resource_group_name=resource_group,
                network_watcher_name=network_watcher_name,
                api_version=self._api_version,
                base_host=self._manager_url,
            )
        else:
            return self._url.format(
                subscription_id=self._subscription_id,
                resource_group_name=resource_group,
                api_version=self._api_version,
                base_host=self._manager_url,
            )

    def _get_source(self, resource_info) -> str:
        if self._resource_type != AzureResourceType.RESOURCE_GRAPH:
            return resource_info.get("id")
        source = resource_info.get("id")
        source_prefix = f"{AzureResourceSource.RESOURCE_GRAPH_COMMON_SOURCE_STR}:{self._account.tenant_id}"
        return f"{source_prefix}:id:{source}" if source else source_prefix

    def _convert_resource_info_list_to_events(self, resource_info_lst):
        events = []
        total_bytes = 0
        for resource_info in resource_info_lst:
            raw = json.dumps(resource_info)
            total_bytes += len(raw.encode())
            events.append(
                dc.build_event(
                    sourcetype=self._sourcetype,
                    source=self._get_source(resource_info),
                    index=self._index,
                    raw_data=raw,
                )
            )
        return events, total_bytes

    def _fetch_events_for_resource(self, url, resource_group=None):
        try:
            next_link = None
            while True:
                self._logger.debug("Sending request url=%s", url)

                # Perform the request
                result = self._perform_request(url)

                # get vm info list in case of Virtual machine only
                if self._resource_type == AzureResourceType.VIRTUAL_MACHINE:
                    resource_info_lst = self._get_detailed_vm_info_list(
                        resource_group, result.get("value")
                    )

                # For resource type we are directly getting the response in result variable
                elif self._resource_type == "resource_groups":
                    resource_info_lst = [result]

                # For other resource type we are getting response in 'value' key.
                else:
                    resource_info_lst = result.get("value")

                # Convert into the events
                events, event_bytes = self._convert_resource_info_list_to_events(
                    resource_info_lst
                )

                # Record observability metrics
                if resource_info_lst:
                    self._obs.record(len(resource_info_lst), event_bytes)

                stop = yield events, None
                if stop:
                    self._logger.info("Received the stop signal.")
                    return

                # check next_link if present, then assign it url variable
                next_link = result.get(self._NEXT_LINK)

                if not next_link:
                    break
                else:
                    url = next_link

        except mae.APIError as e:
            if e.error_code == _RESOURCE_GROUP_NOT_FOUND:
                self._logger.error(
                    f'message="Resource group not found: {e}"', exc_info=e
                )
            else:
                raise e

    def _get_topology(self):
        if any(self._topology_params.values()):
            # Ensure all topology parameters are set
            if not all(self._topology_params.values()):
                missing_fields = [
                    name for name, value in self._topology_params.items() if not value
                ]
                raise ValueError(
                    f"All topology parameters are required to fetch the topology data hence skipping the data collection. missing_parameters={', '.join(missing_fields)}"
                )
            else:
                yield from self._get_manual_topology(
                    self._topology_params.get(
                        mscs_consts.NETWORK_WATCHER_RESOURCE_GROUP
                    ),
                    self._topology_params.get(mscs_consts.NETWORK_WATCHER_NAME),
                    self._topology_params.get(mscs_consts.TARGET_RESOURCE_GROUP),
                )
        else:
            yield from self._get_automatic_topology()

    def _get_automatic_topology(self):
        """Fetch all topology events

        Yields:
            list: Yields list of events
        """
        resource_groups_by_location = self._get_resource_groups_by_location()
        network_watcher_list = self._get_detailed_network_watcher_list()

        for location in resource_groups_by_location:
            # Get the network watcher(s) for this location
            for network_watcher in network_watcher_list:
                if network_watcher.get("location") == location:
                    # Get the resource group and name for this network watcher
                    resource_group_name = re.search(
                        "\/resourceGroups\/(.+?)\/providers", network_watcher["id"]
                    ).group(1)
                    network_watcher_name = network_watcher["name"]
                    url = self._generate_url(
                        resource_group=resource_group_name,
                        network_watcher_name=network_watcher_name,
                    )

                    # Get resource groups in the same location as this watcher
                    for target_resource_group_name in resource_groups_by_location[
                        location
                    ]:
                        # Get the topology for this resource group
                        results = self._get_topology_by_resource_group(
                            url, target_resource_group_name
                        )

                        # Convert result into the events
                        (
                            events,
                            event_bytes,
                        ) = self._convert_resource_info_list_to_events(results)

                        # Record observability metrics
                        if results:
                            self._obs.record(len(results), event_bytes)

                        stop = yield events, None
                        if stop:
                            self._logger.info("Received the stop signal.")
                            return

    def _get_manual_topology(
        self,
        resource_group_name: str,
        network_watcher_name: str,
        target_resource_group_name: str,
    ):
        """Fetch topology events with filter parameters

        Args:
            resource_group_name (str): configured resource group name parameter
            network_watcher_name (str): configured network watcher name parameter
            target_resource_group_name (str): configured target resource group name parameter

        Yields:
            list: yields list of events
        """
        url = self._generate_url(
            resource_group=resource_group_name,
            network_watcher_name=network_watcher_name,
            next_link=None,
        )

        results = self._get_topology_by_resource_group(url, target_resource_group_name)

        # Convert result into the events
        events, event_bytes = self._convert_resource_info_list_to_events(results)

        # Record observability metrics
        if results:
            self._obs.record(len(results), event_bytes)

        stop = yield events, None
        if stop:
            self._logger.info("Received the stop signal.")
            return

    def _get_topology_by_resource_group(
        self, url: str, target_resource_group_name: str
    ) -> list:
        """Fetch topology events based on the target resource group

        Args:
            url (str): API URL to fetch the topology events
            target_resource_group_name (str): target resource group name

        Raises:
            e: Raise exception in case of any error

        Returns:
            list: Returns list of events
        """
        payload = {"targetResourceGroupName": target_resource_group_name}
        topology = self._perform_request(url, method="POST", payload=payload)

        # perform association between resources for visualizations
        # Create a dict with the resource ID as the key so we can look up specific resources later.
        resources = {}

        # 1. Subnets should be associated with virtual networks
        for resource in topology["resources"]:
            # If this is a subnet resource, the VNET is not associated. So, associate it.
            subnet_match = re.search(
                AzureResourceIdPattern.SUBNET_PATTERN, resource["id"]
            )
            if subnet_match:
                vnetId = "/subscriptions/{subscriptionId}/resourceGroups/{resourceGroup}/providers/Microsoft.Network/virtualNetworks/{vnetName}".format(
                    subscriptionId=subnet_match.group("subscriptionId"),
                    resourceGroup=subnet_match.group("resourceGroup"),
                    vnetName=subnet_match.group("vnetName"),
                )
                association = {}
                association["name"] = subnet_match.group("vnetName")
                association["resourceId"] = vnetId
                association["associationType"] = "Associated"
                resource["associations"].append(association)
                resource["vnetId"] = vnetId

            resources[resource["id"]] = resource

        # 2. VMs should be associated with subnets, virtual networks, and security groups. We can get those from the associated NIC.
        # 3. NICs should be associated with VM, security group, subnet, virtual network
        # 4. Security groups should be associated with NIC, VM, virtual network
        try:
            for key, resource in resources.items():
                nic_match = re.search(
                    AzureResourceIdPattern.NIC_PATTERN, resource["id"]
                )
                if nic_match:
                    # Get the subnet, NSG, and VM associated with the NIC
                    subnet_assoc = self._get_topology_assoc(
                        resource, AzureResourceIdPattern.SUBNET_PATTERN
                    )
                    nsg_assoc = self._get_topology_assoc(
                        resource, AzureResourceIdPattern.NSG_PATTERN
                    )
                    vm_assoc = self._get_topology_assoc(
                        resource, AzureResourceIdPattern.VM_PATTERN
                    )

                    # Get the subnet so that we can get the VNET
                    subnet_assoc_res_id = subnet_assoc.get("resourceId")
                    subnet_resource = resources.get(subnet_assoc_res_id)
                    if subnet_resource:
                        vnet_assoc = self._get_topology_assoc(
                            subnet_resource, AzureResourceIdPattern.VNET_PATTERN
                        )

                        # The NIC should be associated with the virtual network
                        if vnet_assoc:
                            resource["associations"].append(vnet_assoc)
                            resource["vnetId"] = vnet_assoc["resourceId"]

                        # The VM should be associated with the subnet, VNET, and NSG
                        if vm_assoc:
                            vm_resource = resources[vm_assoc["resourceId"]]
                            vm_resource["associations"].append(subnet_assoc)
                            if vnet_assoc:
                                vm_resource["associations"].append(vnet_assoc)
                            if nsg_assoc:
                                vm_resource["associations"].append(nsg_assoc)
                            # Let's set the vnetId while we're here
                            vm_resource["vnetId"] = vnet_assoc["resourceId"]

                        # The NSG should be associated with the NIC, VM, and VNET
                        if nsg_assoc:
                            nsg_resource = resources[nsg_assoc["resourceId"]]
                            nic_assoc = {}
                            nic_assoc["name"] = resource["name"]
                            nic_assoc["resourceId"] = resource["id"]
                            # Add "Associated" to showcase the association between resources in the topology
                            nic_assoc["associationType"] = "Associated"
                            nsg_resource["associations"].append(nic_assoc)
                            if vm_assoc:
                                nsg_resource["associations"].append(vm_assoc)
                            nsg_resource["associations"].append(vnet_assoc)
                            # Let's set the vnetID while we're here
                            nsg_resource["vnetId"] = vnet_assoc["resourceId"]
        except Exception as e:
            raise e

        return list(resources.values())

    def _get_topology_assoc(self, resource: dict, pattern: str) -> dict:
        """Get topology association based on the resource_id pattern

        Args:
            resource (dict): Resource details
            pattern (str): Resource ID pattern for matching

        Returns:
            dict: Returns resource
        """
        for assoc in resource["associations"]:
            match = re.search(pattern, assoc["resourceId"])
            if match:
                return assoc
        return None

    def _fetch_events_for_resource_graph(self, subscription_id: str, query: str):
        """Get list of resources

        Args:
            subscription_id (str): subscription_id
            query (str): query to get resources

        Raises:
            e: Raises an exception in case of any error

        Yields:
            list: yields list of events
        """

        payload = {
            "query": query,
            "subscriptions": [subscription_id],
            "options": {"$top": 1000, "$skip": 0},
        }
        headers = str({"Content-type": "application/json"})

        try:
            url = self._url.format(
                api_version=self._api_version, base_host=self._manager_url.strip("/")
            )
            while True:
                skipToken = None
                response = self._perform_request(
                    url, method="POST", payload=payload, headers=headers
                )
                if response["data"]:
                    events, event_bytes = self._convert_resource_info_list_to_events(
                        response["data"]
                    )

                    # Record observability metrics
                    self._obs.record(len(response["data"]), event_bytes)

                    stop = yield events, None
                    if stop:
                        self._logger.info("Received the stop signal.")
                        return

                if "$skipToken" in response:
                    skipToken = response["$skipToken"]
                    payload["options"] = {"$skipToken": skipToken}
                    self._logger.info("skipToken received")

                if not skipToken:
                    break
        except mae.APIError as e:
            raise e
