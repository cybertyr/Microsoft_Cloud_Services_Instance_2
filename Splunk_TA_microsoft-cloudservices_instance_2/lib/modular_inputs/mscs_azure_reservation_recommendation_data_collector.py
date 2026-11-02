#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
import json
import mscs_base_data_collector as mbdc
import mscs_common_api_error as mae
from solnlib.modular_input import event_writer
from solnlib.observability import StanzaObservabilityRecorder
import sys
import signal
import os


class AzureReservationRecommendationDataCollector(mbdc.AzureBaseDataCollector):
    """
    The class responsible for fetching Reservation Recommendation results and its ingestion.

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
        super(AzureReservationRecommendationDataCollector, self).__init__(
            logger, session_key, account_name
        )
        self._parse_api_setting("reservation_recommendation")
        self.ew = event_writer.ClassicEventWriter()
        self.event_ingested = False
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

        # for windows machine
        if os.name == "nt":
            signal.signal(signal.SIGBREAK, self.exit_gracefully)

        # Observability will be lazily initialized
        self._obs = None

    def exit_gracefully(self, signum, frame):
        """
        Handle sigterm gracefully
        """

        sys.exit(0)

    def get_reservation_recommendation_data(self, input_items):
        """
        Fetches the Reservation Recommendation results.

        Args:
            input_items (dict): Input Payload.
        """
        subscription_id = input_items.get("subscription_id")
        try:
            nextLink = None
            while True:
                reservation_responses = {}
                url = self._generate_url(subscription_id, nextLink=nextLink)
                content = self._perform_request(url)
                if "value" in content:
                    reservation_responses = content.get("value")
                self._logger.debug(
                    "Collected total {} events".format(len(reservation_responses))
                )

                yield reservation_responses
                if len(reservation_responses) != 0:
                    self._logger.debug(
                        "Ingested total {} events".format(len(reservation_responses))
                    )

                nextLink = content.get("nextLink") or content.get("@odata.nextLink")
                if nextLink:
                    self._logger.debug("NextLink URL is {}".format(nextLink))
                else:
                    break
        except mae.APIError as e:
            if e.status == 204 and e.error_msg == "No Content":
                self._logger.warning(f'message="{e}"')
            else:
                self._logger.error(f'message="{e}"', exc_info=e)
        except Exception as e:
            raise e

    def _generate_url(self, subscription_id, nextLink=None):
        """
        Generates a valid URL to fetch the Reservation Recommendation results.

        Args:
            subscription_id (str):  A GUID that uniquely identifies your subscription to use Azure services.
            nextLink (str, optional): A valid URL to fetch next set of results. Defaults to None.

        Returns:
            str: A valid URL to fetch the Reservation Recommendation responses.
        """
        if nextLink:
            return nextLink
        return self._url.format(
            api_version=self._api_version,
            subscription_id=subscription_id,
            base_host=self._manager_url.strip("/"),
        )

    def index_reservation_recommendation_data(self, input_items):
        """
        Indexes the Reservation Recommendation results.

        Args:
            input_items (dict): Input Payload.
        """
        stanza_name = input_items["input_name"].split("://", 1)[-1]
        self._obs = StanzaObservabilityRecorder(
            "consumption", self._logger, stanza_name
        )

        with self._obs:
            reservation_responses = self.get_reservation_recommendation_data(
                input_items
            )
            index = input_items.get("index")
            source_type = input_items.get("sourcetype")
            source = ":".join(
                [
                    input_items["input_name"].split("://")[0],
                    "tenant_id",
                    str(self._account.tenant_id),
                ]
            )
            for records in reservation_responses:
                serialized = [json.dumps(r) for r in records]
                event_bytes = sum(len(s.encode()) for s in serialized)
                self.ew.write_events(
                    [
                        self.ew.create_event(
                            data=s,
                            index=index,
                            sourcetype=source_type,
                            source=source,
                        )
                        for s in serialized
                    ]
                )
                self._obs.record(len(serialized), event_bytes)
