#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#

"""
This module has utility functions for fectching account details,
writng events to splunk, setting loggers, proxy details, and api details.
"""

import json
import os.path as op
import sys
import traceback
from logging import Logger, INFO, DEBUG

from solnlib.conf_manager import ConfManagerException

import mscs_consts
import urllib.parse
from solnlib import conf_manager, log
from splunklib import modularinput as smi
from solnlib import utils

from splunk_ta_mscs.models import AzureAccountConfig, format_validation_exception
from cattrs import ClassValidationError, transform_error

APP_NAME = __file__.split(op.sep)[-3]
LOG_FORMAT = (
    "%(asctime)s +0000 log_level=%(levelname)s, pid=%(process)d, tid=%(threadName)s, "
    "file=%(filename)s, func_name=%(funcName)s, code_line_no=%(lineno)d | %(message)s"
)


def get_log_level(session_key):
    """
    This function returns the log level for the addon from configuration file
    :return: The log level configured for the addon
    """

    try:
        cfm = conf_manager.ConfManager(
            session_key,
            APP_NAME,
            realm="__REST_CREDENTIAL__#{}#configs/conf-splunk_ta_mscs_i2_settings".format(
                APP_NAME
            ),
        )
        conf = cfm.get_conf("splunk_ta_mscs_i2_settings")
        logging_details = conf.get("logging")
        log_level = logging_details["agent"]
        if log_level is None:
            log_level = INFO
        return log_level
    except Exception:
        return DEBUG


def set_logger(session_key, filename, log_level=None):
    """
    This function sets up a logger with configured log level.
    :param filename: Name of the log file
    :return logger: logger object
    """

    if not log_level:
        log_level = get_log_level(session_key)

    # To keep consistent log format across all the inputs
    log.Logs.set_context(log_format=LOG_FORMAT)

    logger = log.Logs().get_logger(filename)
    logger.setLevel(log_level)
    return logger


def get_account_from_config(
    logger: Logger, session_key: str, account_name: str
) -> AzureAccountConfig:
    """
    This function retrieves account from addon configuration file
    :param session_key: Session key for the particular modular input
    :param account_name: Account name configured in the addon
    :return: AzureAccount object
    """

    try:
        cfm = conf_manager.ConfManager(
            session_key,
            APP_NAME,
            realm="__REST_CREDENTIAL__#{}#configs/conf-mscs_azure_accounts".format(
                APP_NAME
            ),
        )
        account_conf_file = cfm.get_conf("mscs_azure_accounts")
        logger.debug(
            "Reading account details from mscs_azure_accounts.conf for account name {}".format(
                account_name
            )
        )
        account_details = account_conf_file.get(account_name)
        return AzureAccountConfig.from_dict(account_details)

    except ConfManagerException as e:
        logger.error(
            f'message="Failed to fetch the account details from mscs_azure_accounts.conf file for the account: {account_name}"',
            exc_info=e,
        )
        raise e
    except ClassValidationError as e:
        logger.error(
            f'message="Failed to validate Azure Account model for the account: {account_name}. Error details: {transform_error(e)}"',
            exc_info=e,
        )
        raise e


def get_api_details(logger, session_key, stanza_name):
    """
    This function retrieves api details from addon configuration file
    :param session_key: Session key for the particular modular input
    :param stanza_name: Stanza name to get api details
    :return: Api details in form of a dictionary
    """

    try:
        cfm = conf_manager.ConfManager(
            session_key,
            APP_NAME,
            realm="__REST_CREDENTIAL__#{}#configs/conf-mscs_api_settings".format(
                APP_NAME
            ),
        )
        api_settings_conf_file = cfm.get_conf("mscs_api_settings")
        logger.debug(
            "Reading api details from mscs_api_settings.conf for stanza name {}".format(
                stanza_name
            )
        )

        return {
            "api_version": api_settings_conf_file.get(stanza_name).get("api_version"),
            "url": api_settings_conf_file.get(stanza_name).get("url"),
            "sourcetype": api_settings_conf_file.get(stanza_name).get("sourcetype"),
        }
    except Exception as e:
        logger.error(
            f'message="Failed to fetch the api details from mscs_api_settings.conf file for the stanza name: {stanza_name}"',
            exc_info=e,
        )
        sys.exit("Error while fetching api details. Terminating modular input.")


def write_event(logger, event_writer, raw_event, sourcetype, input_params, manager_url):
    """
    This function ingests data into splunk
    :param event_writer: Event Writer object
    :param raw_event: Raw event to be ingested into splunk
    :param sourcetype: Sourcetype of the data
    :param input_params: Input parameters configured by user
    :param manager_url: URL which is getting used to fetch events
    :return: boolean value indicating if the event is successfully ingested
    """

    try:
        event = smi.Event(
            data=json.dumps(raw_event),
            sourcetype=sourcetype,
            index=input_params["index"],
        )
        event_writer.write_event(event)
        return True
    except Exception as e:
        logger.error('message="Error writing event to Splunk"', exc_info=e)
        return False
