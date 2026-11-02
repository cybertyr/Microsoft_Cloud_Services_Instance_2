#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#

import time
from typing import Callable, Any, List, Optional

from mscs_consts import (
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_RETRYABLE_STATUS_CODES,
    DEFAULT_SLEEP_DURATION,
    MAX_RETRY_AFTER,
    HTTP_THROTTLED,
    HTTP_SERVICE_UNAVAILABLE,
)


class RequestRetrier:
    """Retries HTTP requests on configured error codes"""

    def __init__(
        self,
        logger,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        retryable_errors: List[int] = DEFAULT_RETRYABLE_STATUS_CODES,
        default_sleep_duration: int = DEFAULT_SLEEP_DURATION,
    ):
        self._logger = logger
        self._default_max_attempts = max_attempts
        self._retryable_errors = retryable_errors
        self._default_sleep_duration = default_sleep_duration

    def execute_with_retry(
        self,
        request_func: Callable,
        max_attempts: int = None,
    ) -> Any:
        attempts = (
            max_attempts if max_attempts is not None else self._default_max_attempts
        )
        response = None

        for attempt in range(1, attempts + 1):
            response = request_func()

            self._log_rate_limit_headers(response)

            if not self._is_retryable_error(response.status_code):
                self._log_completion_after_retry(attempt)
                return response

            if self._is_last_attempt(attempt, attempts):
                self._log_last_attempt(response.status_code, attempts)
                return response

            sleep_duration = self._get_sleep_duration(response)
            self._log_retry_attempt(
                response.status_code, sleep_duration, attempt, attempts
            )
            time.sleep(sleep_duration)

        return response

    def _is_retryable_error(self, status_code: int) -> bool:
        return status_code in self._retryable_errors

    def _is_last_attempt(self, attempt: int, max_attempts: int) -> bool:
        return attempt == max_attempts

    def _get_sleep_duration(self, response) -> int:
        headers = getattr(response, "headers", None)
        if not headers:
            return self._default_sleep_duration

        header_name, value = self._find_retry_after_header(headers)
        if not header_name:
            return self._default_sleep_duration

        return self._parse_and_validate_duration(header_name, value)

    def _find_retry_after_header(
        self, headers: dict
    ) -> tuple[Optional[str], Optional[str]]:
        retry_after = headers.get("Retry-After")
        if retry_after:
            return "Retry-After", retry_after

        for header_name in headers:
            if "retry-after" in header_name.lower():
                return header_name, headers[header_name]

        return None, None

    def _parse_and_validate_duration(self, header_name: str, value: str) -> int:
        try:
            duration = int(value)
        except (ValueError, TypeError):
            self._logger.warning(
                f"message=\"Invalid Retry-After header value in '{header_name}': {value}, using default\""
            )
            return self._default_sleep_duration

        if duration < 0:
            self._logger.warning(
                f"message=\"Negative Retry-After value in header '{header_name}': {duration}, using default\""
            )
            return self._default_sleep_duration

        if duration > MAX_RETRY_AFTER:
            self._logger.warning(
                f"message=\"Retry-After in header '{header_name}' exceeds max: {duration}s > {MAX_RETRY_AFTER}s, capping to {MAX_RETRY_AFTER}s\""
            )
            return MAX_RETRY_AFTER

        self._logger.debug(
            f"message=\"Using Retry-After from header '{header_name}': {duration}s\""
        )
        return duration

    def _log_last_attempt(self, status_code: int, max_attempts: int):
        message = self._get_retry_message(status_code)
        self._logger.warning(
            f'message="{message}. Reached maximum number of attempts: ({max_attempts})"'
        )

    def _log_retry_attempt(
        self, status_code: int, sleep_duration: int, attempt: int, max_attempts: int
    ):
        message = self._get_retry_message(status_code)
        self._logger.info(
            f'message="{message}. Retrying after {sleep_duration}s. Attempt {attempt}/{max_attempts}"'
        )

    def _log_completion_after_retry(self, attempt: int):
        if attempt > 1:
            self._logger.info(
                f'message="Request completed after retry. Attempts: {attempt}"'
            )

    def _get_retry_message(self, status_code: int) -> str:
        messages = {
            HTTP_THROTTLED: "Throttling limit reached (429)",
            HTTP_SERVICE_UNAVAILABLE: "Service unavailable (503)",
        }
        return messages.get(status_code, f"Error ({status_code})")

    def _log_rate_limit_headers(self, response):
        headers = getattr(response, "headers", None)
        if not headers:
            return
        quota_info = []

        retry_after = headers.get("Retry-After")
        if retry_after:
            quota_info.append(f"Retry-After={retry_after}s")

        # Subscription-level read quota
        sub_reads = headers.get("x-ms-ratelimit-remaining-subscription-reads")
        if sub_reads:
            quota_info.append(f"sub_reads={sub_reads}")

        # Subscription-level resource entities read quota
        sub_resource_reads = headers.get(
            "x-ms-ratelimit-remaining-subscription-resource-entities-read"
        )
        if sub_resource_reads:
            quota_info.append(f"sub_resource_reads={sub_resource_reads}")

        # Tenant-level read quota
        tenant_reads = headers.get("x-ms-ratelimit-remaining-tenant-reads")
        if tenant_reads:
            quota_info.append(f"tenant_reads={tenant_reads}")

        # Tenant-level resource entities read quota
        tenant_resource_reads = headers.get(
            "x-ms-ratelimit-remaining-tenant-resource-entities-read"
        )
        if tenant_resource_reads:
            quota_info.append(f"tenant_resource_reads={tenant_resource_reads}")

        if quota_info:
            self._logger.debug(f'message="Azure quota: {", ".join(quota_info)}"')
