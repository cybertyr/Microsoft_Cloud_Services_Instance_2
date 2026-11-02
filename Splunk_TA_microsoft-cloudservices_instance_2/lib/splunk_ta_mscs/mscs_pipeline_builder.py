#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#

from azure.core.pipeline import Pipeline
from azure.core.pipeline.transport import RequestsTransport
from azure.core.pipeline.policies import (
    RetryPolicy,
    BearerTokenCredentialPolicy,
    ProxyPolicy,
)
from splunk_ta_mscs.models import ProxyConfig

SCOPES = "https://management.azure.com//.default"
DEFAULT_TIMEOUT = 120
DEFAULT_RETRIES = 0


class PipelineBuilder:
    def __init__(self, credentials, proxies: ProxyConfig, scopes: str = SCOPES) -> None:
        self._transport = RequestsTransport()
        self._base_policies = [
            ProxyPolicy(proxies.proxy_dict),
            BearerTokenCredentialPolicy(credentials, scopes),
        ]
        self._retry_policy = RetryPolicy(
            timeout=DEFAULT_TIMEOUT, retry_total=DEFAULT_RETRIES
        )

    def with_retries(self, timeout=DEFAULT_TIMEOUT, retry_total=DEFAULT_RETRIES):
        self._retry_policy = RetryPolicy(timeout=timeout, retry_total=retry_total)
        return self

    def build(self) -> Pipeline:
        policies = self._base_policies + [self._retry_policy]
        pipeline = Pipeline(transport=self._transport, policies=policies)
        return pipeline
