#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
import contextlib
import json
import traceback
import signal
import os
import time
import sys

import mscs_base_data_collector as mbdc
import mscs_common_api_error as mcae

from solnlib.modular_input import event_writer

import mscs_consts
from solnlib.observability import StanzaObservabilityRecorder

VALIDATION_TIMEOUT_SEC = 120
VALIDATION_RETRIES = 0
COLLECTION_TIMEOUT_SEC = 300


class AzureKQLCollector(mbdc.AzureBaseDataCollector):
    def __init__(self, input_stanza, session_key, logger):
        """Initialize class with details regarding account, proxy, logging, etc."""
        self.logger = logger

        self.input_name = input_stanza.get("name")
        self.input_type = input_stanza.get("input_type")
        self.account_name = input_stanza.get("account")
        self.workspace_id = input_stanza.get("workspace_id")
        self.kql_query = input_stanza.get("kql_query")
        self.index_stats = input_stanza.get("index_stats")
        self.index = input_stanza.get("index")
        self.sourcetype = input_stanza.get("sourcetype")
        self.stats_sourcetype = f"{self.sourcetype}:stats"
        self.index_empty_values = input_stanza.get("index_empty_values")

        # ClassicEventWriter supports batch event writing
        self.ew = event_writer.ClassicEventWriter()

        super(AzureKQLCollector, self).__init__(
            logger,
            session_key,
            self.account_name,
            endpoint=mscs_consts.LOG_ANALYTICS_ENDPOINT,
        )

        self._parse_api_setting("kql_log_analytics")

        self.source = f"{self.input_type}:tenant_id:{self._account.tenant_id}"

        stanza_name = (self.input_name or "").split("://", 1)[-1]
        self._obs = (
            StanzaObservabilityRecorder("kql", self.logger, stanza_name)
            if stanza_name
            else None
        )

    def _fetch_kql_events(self, url, body, headers, log_partial_err=True, **kwargs):
        """Fetch KQL events from API."""
        self.logger.info(f"Fetching Log Analytics events")
        result = None
        st = time.time()
        try:
            result = self._perform_request(
                url, method="post", body=body, headers=headers, **kwargs
            )
        except mcae.APIError as e:
            if e.status == 200:
                # if len(total_events) > max_records_limit,
                # then truncation happens and error comes along with max possible data
                # Default: truncationmaxsize=500K | truncationmaxrecords=64MB
                if log_partial_err:
                    self.logger.error(
                        f'message="Received error along with events, status code: {e.status}, error: {json.dumps(e.error)}"',
                        exc_info=e,
                    )
                result = e.result
            else:
                raise

        et = time.time()
        spent = et - st
        self.logger.debug(f"Time taken by API call, spent={round(spent, 6)}")

        return result

    def _extract_table_events(self, result):
        """Extract KQL table events from API response."""
        self.logger.info(f"Extracting table events")
        events = []
        empty_events = 0
        for table in result.get("tables", []):
            cols = [col["name"] for col in table.get("columns", [])]
            col_len = len(cols)

            for row in table.get("rows", []):
                event = {}
                for idx in range(col_len):
                    val = row[idx]
                    try:
                        event[cols[idx]] = json.loads(val)
                    except Exception:
                        if (
                            self.index_empty_values
                            or (not isinstance(val, str))
                            or (val.strip() != "")
                        ):
                            event[cols[idx]] = val
                        else:
                            continue

                if not event:
                    empty_events += 1
                    continue

                events.append(event)

        if empty_events:
            self.logger.info(f"Number of excluded empty events, count={empty_events}")

        self.logger.info(f"Extracted table events, count={len(events)}")
        return events

    def _extract_kql_stats_event(self, result):
        """Extract KQL query statistics event from API response."""
        self.logger.info(f"Extracting KQL query statistics event")
        stats_event = {}
        if not result.get("statistics"):
            self.logger.info("No KQL query statistics found in result")
            return stats_event

        stats_event = result.get("statistics", {})
        stats_event["inputQuery"] = self.kql_query
        stats_event["inputName"] = self.input_name

        self.logger.info(f"Extracted KQL query statistics event")
        return stats_event

    def _ingest_events(self, events, sourcetype, event_type="input"):
        """Ingest given events into Splunk."""
        self.logger.info(f"Ingesting {event_type} events")
        serialized = [json.dumps(event) for event in events]
        total_bytes = sum(len(s.encode()) for s in serialized)
        splunk_events = [
            self.ew.create_event(
                data=s,
                index=self.index,
                sourcetype=sourcetype,
                source=self.source,
            )
            for s in serialized
        ]
        try:
            self.ew.write_events(splunk_events)
            self.logger.info(
                f"Ingested {event_type} events, count={len(splunk_events)}"
            )
            if self._obs is not None:
                self._obs.record(len(splunk_events), total_bytes)
        except BrokenPipeError as e:
            self.logger.error(
                f'message="Error occurred during {event_type} events ingestion, events might be ingested partially: {e}"',
                exc_info=e,
            )
            raise

    def _get_api_url(self):
        """Get API endpoint url."""
        return self._url.format(
            base_host=self._manager_url.strip("/"),
            api_version=self._api_version,
            workspace_id=self.workspace_id,
        )

    def _get_api_body(self, max_records=None):
        """Get API Body to pass in API calling."""
        kql_query = self.kql_query

        if isinstance(max_records, int):
            # "set" command as a suffix takes priority over prefix, if supplied at both end
            # multiple semi-colon (;) results in syntax error of KQL query
            kql_query = kql_query.strip()
            delim = ""
            if len(kql_query) and kql_query[-1] != ";":
                delim = ";"

            kql_query = f"{kql_query}{delim} set truncationmaxrecords={max_records};"

        body = {"query": kql_query}
        return json.dumps(body)

    def _get_api_headers(self):
        """Get API Headers to pass in API calling."""
        headers = {"Content-Type": "application/json"}
        if self.index_stats:
            headers.update({"Prefer": "include-statistics=true"})
        return headers

    def exit_gracefully(self, signum, frame):
        """Handle termination gracefully on receiving OS signal."""
        self.logger.info(
            f"Received OS signal for termination (signum: {signum}), exiting gracefully."
        )
        sys.exit(0)

    def handle_os_signals(self):
        """Handle various OS signal."""
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

        # for windows machine
        if os.name == "nt":
            signal.signal(signal.SIGBREAK, self.exit_gracefully)

    def validate_kql_query(self):
        """Validate the user provided KQL query."""
        self.logger.info("Validating the KQL query")
        url = self._get_api_url()

        # keep max_records's value minimum to test KQL query without much data transfer
        # info: max_records needs to be greater than 0 else API ignores it
        body = self._get_api_body(max_records=1)
        headers = self._get_api_headers()
        result = self._fetch_kql_events(
            url,
            body,
            headers,
            log_partial_err=False,
            timeout=VALIDATION_TIMEOUT_SEC,
            retries=VALIDATION_RETRIES,
        )

        table_events = self._extract_table_events(result)

        return True

    def start(self):
        """Start data collection."""
        try:
            self.logger.info("Starting the data collection")
            self.handle_os_signals()

            with self._obs if self._obs is not None else contextlib.nullcontext():
                url = self._get_api_url()
                body = self._get_api_body()
                headers = self._get_api_headers()
                result = self._fetch_kql_events(
                    url, body, headers, timeout=COLLECTION_TIMEOUT_SEC
                )

                table_events = self._extract_table_events(result)
                self._ingest_events(table_events, self.sourcetype, event_type="table")

                if self.index_stats:
                    kql_stats_events = [self._extract_kql_stats_event(result)]
                    self._ingest_events(
                        kql_stats_events,
                        self.stats_sourcetype,
                        event_type="KQL statistics",
                    )

        except Exception as e:
            self.logger.error(f'message="Unknown error occurred: {e}"', exc_info=e)

        else:
            self.logger.info("Completed the data collection")
