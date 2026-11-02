#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
import json
import datetime
import mscs_base_data_collector as mbdc
from solnlib.modular_input import checkpointer, event_writer
import mscs_common_api_error as mae
from solnlib.observability import StanzaObservabilityRecorder
import sys
import signal
import os

DATE_DAYS_AGO = datetime.datetime.now(
    datetime.timezone.utc
).date() - datetime.timedelta(days=1)


class StartDateError(Exception):
    pass


class QueryWindowTooLargeError(Exception):
    pass


class AzureUsageDetailsDataCollector(mbdc.AzureBaseDataCollector):
    """
    The class responsible for fetching Usage Details results and its ingestion.

    Args:
        mbdc (Any): AzureBaseDataCollector class which is responsible for making API requests.
    """

    def __init__(self, logger, session_key, account_name):
        """
        Args:
            logger (Any): Logger object for logging in file.
            session_key (Any): Session key for the particular modular input.
            account_name (str): Account name configured in the addon.
        """
        super(AzureUsageDetailsDataCollector, self).__init__(
            logger, session_key, account_name
        )
        self._parse_api_setting("usage_details")
        self._ckpt = checkpointer.KVStoreCheckpointer(
            "splunk_ta_mscs_i2_azure_consumption",
            self._session_key,
            "Splunk_TA_microsoft-cloudservices_instance_2",
        )
        self._checkpoint_data = {}
        self.ew = event_writer.ClassicEventWriter()
        self.checkpoint_updated = False
        self.event_ingested = False
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

        # for windows machine
        if os.name == "nt":
            signal.signal(signal.SIGBREAK, self.exit_gracefully)

        self._obs = None

    def exit_gracefully(self, signum, frame):
        """
        Handle sigterm gracefully and update the checkpoint
        if events are ingested and checkpoint is not updated to restrict data duplication
        """
        try:
            if self.event_ingested and not self.checkpoint_updated:
                self._ckpt.update(self.checkpoint_key, self._checkpoint_data)
                self._logger.info(
                    "Checkpoint for input saved before termination due to SIGTERM, "
                    f"with value = {self._checkpoint_data}"
                )
        except Exception as e:
            self._logger.error(f'message="SIGTERM termination error: {e}"', exc_info=e)
        sys.exit(0)

    def get_start_date(self, input_items):
        """
        Fetches the Start Date value from user provided value
        If the Start Date  isn't available the default date is 90 days
        in the past.

        Args:
            input_items (dict): Input Payload.

        Returns:
            str: Start Date.
        """
        # If a start date was specified as an argument take it
        # Providing 90 days in the past value in case of input creation from backend
        start_date = input_items.get("start_date") or (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(90)
        ).strftime("%Y-%m-%d")
        try:
            start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
            past_date = datetime.datetime.strptime("2014-05-01", "%Y-%m-%d").date()
            now = datetime.datetime.now(datetime.timezone.utc).date()
            if start_date < past_date:
                raise StartDateError("'Start Date' cannot be older than May 1, 2014")
            if start_date > now:
                raise StartDateError("'Start Date' cannot be in the future")
        except ValueError:
            raise ValueError(
                "Invalid 'Start Date' {}. Please enter a valid 'Start Date' in the YYYY-MM-DD format.".format(
                    start_date
                )
            )
        return start_date.strftime("%Y-%m-%d")

    def get_end_date(self, start_date, query_days):
        """
        Fetches the End Date value.

        Args:
            start_date (str): Start Date value.
            query_days (int): Number of days to query.Used in End Date calculation.(start_date + query_days)

        Returns:
            str: End Date
        """
        date_start = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
        try:
            date_end = date_start + datetime.timedelta(days=query_days)
        except Exception:
            raise QueryWindowTooLargeError(
                "The maximum query window size for this operation has been exceeded. Please try a smaller time period."
            )

        if date_start > DATE_DAYS_AGO:
            self._logger.debug(
                "Start date '%s' is greater than the '%s'. Hence Skipping..."
                % (date_start.strftime("%Y-%m-%d"), DATE_DAYS_AGO.strftime("%Y-%m-%d"))
            )
            return None
        # Adjust the end date if we went too far.
        if date_end > DATE_DAYS_AGO:
            date_end = DATE_DAYS_AGO

        self._logger.debug(
            "Collecting the data between Start Date '%s' and End Date '%s'."
            % (date_start.strftime("%Y-%m-%d"), date_end.strftime("%Y-%m-%d"))
        )
        return date_end.strftime("%Y-%m-%d")

    def prepare_response(self, subscription_id, start_date, end_date, nextLink):
        """
        Fetches the response from provided url.
        Pagination is achieved via the 'nextLink' parameter in
        the response.

        Args:
            subscription_id (str):  A GUID that uniquely identifies your subscription to use Azure services.
            start_date (str): A valid Start Date.
            end_date (str): A valid End Date.
            nextLink (str, optional): A valid URL to fetch next set of results.

        Returns:
            Tuple: A tuple containing nextLink expiration status,nextLink and response content.
        """
        if nextLink:
            url = self._generate_url(subscription_id, nextLink)
        else:
            url = self._generate_url(subscription_id)
        try:
            params = (
                None
                if nextLink
                else {
                    "$orderby": "properties/usageEnd",
                    "$expand": "properties/meterDetails,properties/additionalInfo",
                    "$filter": "properties/usageStart ge '{}' AND properties/usageEnd le '{}'".format(
                        start_date, end_date
                    ),
                }
            )
            response = self._perform_request(url, params=params)
        except mae.APIError as e:
            if e.status == 204 and e.error_msg == "No Content":
                self._logger.warning(f'message="{e}"')
                return False, "", {}

            if e.status == 400 and "has expired" in e.error_msg:
                self._logger.error(f'message="{e}"', exc_info=e)
                return True, "", {}

            self._logger.error(f"message={e}", exc_info=e)
            raise e
        except Exception as e:
            self._logger.error(
                f'message="An error occured while fetching usage details data: {e}"',
                exc_info=e,
            )
            raise e
        return False, response.get("nextLink"), response

    def get_usage_details_data(self, input_items):
        """
        Fetches the Usage Details results.

        Args:
            input_items (dict): Input Payload.
        """
        subscription_id = input_items.get("subscription_id")
        query_days = int(input_items.get("query_days", 10))
        self._checkpoint_data = self._ckpt.get(self.checkpoint_key) or {}

        while True:
            if not self._checkpoint_data:
                start_date = self.get_start_date(input_items)
                self._checkpoint_data = {
                    "start_date": start_date,
                    "end_date": "",
                    "nextLink": "",
                }
            else:
                self._logger.debug(
                    "Found existing checkpoint for input {},checkpoint value: {}".format(
                        self.checkpoint_key, self._checkpoint_data
                    )
                )

            end_date = self._checkpoint_data["end_date"]
            nextLink = self._checkpoint_data["nextLink"]
            if not end_date:
                self._checkpoint_data["end_date"] = end_date = self.get_end_date(
                    self._checkpoint_data["start_date"], query_days
                )

            if end_date is None:
                break

            else:
                is_nextLink_expired, nextLink, content = self.prepare_response(
                    subscription_id,
                    self._checkpoint_data["start_date"],
                    end_date,
                    nextLink,
                )

            if is_nextLink_expired:
                """
                If the nextLink expires the data for the whole query window will be collected again.
                """
                self._checkpoint_data.update({"nextLink": ""})
                self._ckpt.update(self.checkpoint_key, self._checkpoint_data)
                self._logger.debug(
                    "Updated checkpoint for input {} with value {}, As stored nextLink has expired.This may lead to data duplication.".format(
                        self.checkpoint_key, self._checkpoint_data
                    )
                )
                continue

            usage_details_responses = {}
            if "value" in content:
                usage_details_responses = content.get("value")

            if nextLink:
                self._logger.debug("NextLink URL is %s" % (nextLink))
                self._checkpoint_data.update({"nextLink": nextLink})

            else:
                """
                If the nextLink doesn't have any value that indicates data for that query window
                is collected completely.
                """
                end_date = (
                    datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
                    + datetime.timedelta(days=1)
                ).strftime("%Y-%m-%d")
                self._checkpoint_data.update(
                    {"start_date": end_date, "end_date": "", "nextLink": ""}
                )

            self.checkpoint_updated = False
            self._logger.debug(
                "Collected total {} events".format(len(usage_details_responses))
            )
            yield usage_details_responses
            self.events_ingested = True

            if len(usage_details_responses) != 0:
                self._logger.debug(
                    "Ingested total {} events".format(len(usage_details_responses))
                )

            self._ckpt.update(self.checkpoint_key, self._checkpoint_data)
            self.checkpoint_updated = True
            self._logger.debug(
                "Updated checkpoint for input {} with value {}".format(
                    self.checkpoint_key, self._checkpoint_data
                )
            )
            self.event_ingested = False

    def _generate_url(self, subscription_id, nextLink=None):
        """
        Generates a valid URL to fetch the Usage Details results between Start Date and End Date.

        Args:
            subscription_id (str):  A GUID that uniquely identifies your subscription to use Azure services.
            nextLink (str, optional): A valid URL to fetch next set of results. Defaults to None.

        Returns:
            str: A valid URL to fetch the Usage Detail responses.
        """
        if nextLink:
            return nextLink
        return self._url.format(
            api_version=self._api_version,
            subscription_id=subscription_id,
            base_host=self._manager_url.strip("/"),
        )

    def index_usage_details_data(self, input_items):
        """
        Indexes the Usage Details results.

        Args:
            input_items (dict): Input Payload.
        """
        self.checkpoint_key = input_items["input_name"].replace("://", "_")
        stanza_name = input_items["input_name"].split("://", 1)[-1]
        self._obs = StanzaObservabilityRecorder(
            "consumption", self._logger, stanza_name
        )

        with self._obs:
            usage_details_responses = self.get_usage_details_data(input_items)
            for records in usage_details_responses:
                serialized = [json.dumps(r) for r in records]
                event_bytes = sum(len(s.encode()) for s in serialized)
                self.ew.write_events(
                    [
                        self.ew.create_event(
                            data=s,
                            index=input_items.get("index"),
                            sourcetype=input_items.get("sourcetype"),
                            source=input_items["input_name"].split("://")[0],
                        )
                        for s in serialized
                    ]
                )
                self._obs.record(len(serialized), event_bytes)
