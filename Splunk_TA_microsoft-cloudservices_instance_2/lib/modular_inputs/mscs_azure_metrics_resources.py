#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
import json
import mscs_base_data_collector as mbdc
import mscs_common_api_error as mcae


class AzureMetricsResourceCollector(mbdc.AzureBaseDataCollector):
    """
    class responsible for fetching resources
    Args:
        mbdc: AzureBaseDataCollector class is responsible for making API requests
    """

    def __init__(self, logger, session_key, account_name):
        """
        Init method
        :param logger: logger object for logging in file
        :param session_key: session key
        :param account_name: configured account
        """
        super(AzureMetricsResourceCollector, self).__init__(
            logger, session_key, account_name
        )
        self._parse_api_setting("metrics_resources")

    def get_resources_by_query(self, query, subscription_ids):
        """
        Get list of resources
        :param query: query to get resources
        :param subscription_ids: subscription_ids for the namespaces
        :return list of resources
        """
        data = {
            "query": query,
            "subscriptions": subscription_ids,
            "options": {"$top": 10000, "$skip": 0},
        }
        headers = {"Content-type": "application/json"}

        try:
            resources = []
            url = self._generate_url()
            while True:
                skipToken = None
                response = self._perform_request(
                    url, "post", body=json.dumps(data), headers=headers
                )
                if response["data"]:
                    resources.extend(response["data"])

                if "$skipToken" in response:
                    skipToken = response["$skipToken"]
                    data["options"] = {"$skipToken": skipToken}
                    self._logger.debug("skipToken received")

                # significance of resultTruncated not mentioned in the API docs
                if response.get("resultTruncated", "false").lower() == "true":
                    self._logger.warning(
                        'message="Received resultTruncated value as true in response"'
                    )

                if not skipToken:
                    break
        except mcae.APIError as e:
            self._logger.error(
                f'message="Error occurred while fetching resources by query - status_code: {str(e.status)}, error: {str(e.result)}"',
                exc_info=e,
            )
            raise

        return resources

    def _generate_url(self):
        """
        generate url
        :return url to get resources based on query
        """
        return self._url.format(
            api_version=self._api_version, base_host=self._manager_url.strip("/")
        )
