#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
import logging


class PrefixedLoggerAdapter(logging.LoggerAdapter):
    """
    The logger adapter wraps a logger instance and padding a log prefix for
    every log message.
    """

    def __init__(self, logger: logging.Logger, prefix: str = "", **kwargs):
        super(PrefixedLoggerAdapter, self).__init__(logger, {})
        self.prefix = self._get_combined_prefix(prefix, **kwargs)

    def process(self, msg, kwargs):
        return "%s %s" % (self.prefix, msg), kwargs

    @staticmethod
    def _get_combined_prefix(prefix: str = "", **kwargs) -> str:
        if not prefix and not kwargs:
            return ""
        prefix = prefix.strip("[]")
        kwargs_prefix = " ".join(f"{k}={v}" for k, v in kwargs.items())
        return "[" + " ".join([prefix, kwargs_prefix]).strip() + "]"


def logger_for(log_prefix: str = "", **kwargs: dict) -> PrefixedLoggerAdapter:
    """
    Wrap the default logger instance with a prefix.

    :param log_prefix: The prefix to be inserted to the front of log.
    :param kwargs: Kwargs parameters to be added into the log prefix.
    :return: A PrefixedLoggerAdapter
    """
    from splunktaucclib.common.log import logger

    return PrefixedLoggerAdapter(logger, log_prefix, **kwargs)
