#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
class APIError(Exception):
    def __init__(
        self,
        status=None,
        error_code=None,
        error_msg=None,
        innererror={},
        result=None,
        response=None,
        error=None,
    ):
        self.status = status
        self.error_code = error_code
        self.error_msg = error_msg
        self.innererror = innererror
        self.inner_error_msg = self.innererror.get("innererror", {}).get("message", "")
        self.result = result
        self.response = response
        self.error = error

    def __str__(self):
        return "status_code={}, error_code={}, error_msg={} inner_error_msg={}".format(
            self.status, self.error_code, self.error_msg, self.inner_error_msg or "{}"
        )
