#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#

from azure.core.rest import HttpRequest
from splunk_ta_mscs.models import ProxyConfig

from splunk_ta_mscs.mscs_pipeline_builder import (
    PipelineBuilder,
    SCOPES,
    DEFAULT_TIMEOUT,
)


class ServiceClient:
    def __init__(self, credentials, proxies: ProxyConfig, scopes: str = SCOPES) -> None:
        self._pipeline_builder = PipelineBuilder(credentials, proxies, scopes)

    def send(self, request: HttpRequest, timeout: int = DEFAULT_TIMEOUT, **kwargs):
        pipeline = self._pipeline_builder.with_retries(timeout=timeout).build()

        response = pipeline.run(request, **kwargs)
        return response.http_response
