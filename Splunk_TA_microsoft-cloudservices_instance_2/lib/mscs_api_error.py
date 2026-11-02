#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
class APIError(Exception):
    def __init__(self, status, error_code, error_msg):
        self.status = status
        self.error_code = error_code
        self.error_msg = error_msg

    def __str__(self):
        return repr(
            "status={status}, error_code={error_code}, error_msg={error_msg}".format(
                status=self.status, error_code=self.error_code, error_msg=self.error_msg
            )
        )
