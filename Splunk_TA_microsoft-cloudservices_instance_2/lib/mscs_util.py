#!/usr/bin/python
#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
import calendar
import datetime
import json
import logging
import math
import os
import os.path as op
import threading
import time
from functools import wraps
import hashlib

import solnlib.utils
from splunktaucclib.common.log import set_log_level
import pymd5

from splunktaucclib.rest_handler.endpoint import (
    field,
    validator,
)

import mscs_consts
from solnlib.splunkenv import get_splunkd_uri  # pylint: disable=import-error

from splunk_ta_mscs_i2.models import ProxyConfig
from splunk import admin, clilib, rest  # pylint: disable=import-error

from solnlib import (  # isort: skip # pylint: disable=import-error
    conf_manager,
    time_parser,
    utils,
)

from codecs import (  # isort: skip
    BOM_UTF8,
    BOM_UTF16_BE,
    BOM_UTF16_LE,
    BOM_UTF32_BE,
    BOM_UTF32_LE,
)

from splunksdc.config import Field

g_time_parser = None


default_logger = logging.getLogger("mscs_util")
default_logger.addHandler(logging.NullHandler())


class IsCancelledException(Exception):
    pass


def _make_log_file_path(filename):
    """
    The replacement for make_splunkhome_path in splunk.appserver.mrsparkle.lib.util
    Importing the package above will corrupted the sys.path.
    """

    home = os.environ.get("SPLUNK_HOME", "")
    return op.join(home, "var", "log", "splunk", filename)


class SessionKeyProvider(admin.MConfigHandler):
    """
    Works only if you import __main__ and set __main__.___sessionKey to the auth token.
    As this is the logic of getSessionKey
    """

    def __init__(self):
        self.session_key = self.getSessionKey()


def get_proxy_info_from_endpoint(session_key=None) -> ProxyConfig:
    if not session_key:
        session_key = SessionKeyProvider().session_key

    splunkd_uri = get_splunkd_uri(session_key)
    rest_endpoint = (
        splunkd_uri
        + "/servicesNS/nobody/Splunk_TA_microsoft-cloudservices_instance_2/splunk_ta_mscs_i2_settings/proxy?--cred--=1&"
        "output_mode=json"
    )

    response, content = rest.simpleRequest(
        rest_endpoint, sessionKey=session_key, method="GET", raiseAllErrors=True
    )
    proxy_settings = json.loads(content)["entry"][0]["content"]

    return ProxyConfig.from_dict(proxy_settings)


def get_conf_file_info(session_key, conf_file_name, only_current_app=False):
    cfm = conf_manager.ConfManager(
        session_key,
        "Splunk_TA_microsoft-cloudservices_instance_2",
        realm="__REST_CREDENTIAL__#Splunk_TA_microsoft-cloudservices_instance_2#configs/conf-{}".format(
            conf_file_name
        ),
    )

    conf = cfm.get_conf(conf_file_name)
    configs = conf.get_all(only_current_app)
    return configs


def check_account_secret_isvalid(confInfo, session_id, account_type, storage_account):
    """
    Check whether each secret has been valid or not
    """

    if account_type == "storage":
        conf_file_name = "mscs_storage_accounts"
        credential_field = "account_secret"
    else:
        raise ValueError("Invalid credential_field: Possible values are ['storage']")

    try:
        cfm = conf_manager.ConfManager(
            session_id,
            "Splunk_TA_microsoft-cloudservices_instance_2",
            realm="__REST_CREDENTIAL__#Splunk_TA_microsoft-cloudservices_instance_2#configs/conf-{}".format(
                conf_file_name
            ),
        )
        # Get Conf object of account settings
        conf = cfm.get_conf(conf_file_name)
        # Get account stanza from the settings
        account_configs = conf.get_all()
        if (
            account_configs[storage_account[0]].account_secret_type == "0"
        ):  # None Secret
            return False
        return True

    except conf_manager.ConfManagerException:
        # For fresh addon account conf file will not exist so handling that exception
        pass


def timestamp_to_localtime(session_key, timestamp):
    global g_time_parser
    if not g_time_parser:
        g_time_parser = time_parser.TimeParser(session_key)
    utc_str = timestamp_to_utc(timestamp)
    local_str = g_time_parser.to_local(utc_str)
    return local_str[0:19] + local_str[23:]


def timestamp_to_utc(timestamp):
    utc_time = datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)
    return utc_time.strftime("%Y-%m-%dT%H:%M:%SZ")


def utc_timestr_to_timestamp(utc_timestr: str):
    utc_datetime = datetime.datetime.strptime(utc_timestr, "%Y-%m-%dT%H:%M:%S%z")
    sec_part = calendar.timegm(utc_datetime.timetuple())
    return int(sec_part)


def get_30_days_ago_local_time(session_key):
    cur_time = time.time()
    before_time = cur_time - 30 * 24 * 60 * 60
    return timestamp_to_localtime(session_key, before_time)


def decode_ascii_str(ascii_str):
    decoded_str = ""
    len_str = len(ascii_str)
    i = 0
    while i < len_str:
        if ascii_str[i] == ":" and i + 4 < len_str:
            decoded_str += chr(int(ascii_str[i + 1 : i + 5], 16))  # noqa: E203
            i += 5
        else:
            decoded_str += ascii_str[i]
            i += 1
    return decoded_str


BOMS = (
    (BOM_UTF8, "UTF-8"),
    (BOM_UTF32_BE, "UTF-32-BE"),
    (BOM_UTF32_LE, "UTF-32-LE"),
    (BOM_UTF16_BE, "UTF-16-BE"),
    (BOM_UTF16_LE, "UTF-16-LE"),
)


def check_bom(data):
    # http://unicodebook.readthedocs.io/guess_encoding.html
    return [encoding for bom, encoding in BOMS if data.startswith(bom)]


def get_schema_file_path(filename):
    return op.join(op.dirname(op.abspath(__file__)), filename)


def setup_log_level(log_level=None):
    """
    Set the log level of the logging
    """
    if not log_level:
        cfm = clilib.cli_common.getConfStanza("splunk_ta_mscs_i2_settings", "logging")
        log_level = cfm.get("agent")
    set_log_level(log_level)


MscsAzureIndexValidator = lambda: validator.AllOf(
    validator.Pattern(
        regex=r"^(~\.federated\.)?[a-zA-Z0-9][a-zA-Z0-9\_\-]*$",
    ),
    validator.String(
        max_len=1023,
        min_len=1,
    ),
)


class StartTimeValidator(validator.Validator):
    def __init__(self, max_days_in_the_past: int = 90):
        super().__init__()
        self.max_days_in_the_past = max_days_in_the_past

    def validate(self, value, data):
        try:
            input_datetime_utc = datetime.datetime.strptime(
                data.get("start_time"), "%Y-%m-%dT%H:%M:%S%z"
            )

        except Exception as e:
            self.put_msg(
                'Incorrect format for "start_time". Correct format is: YYYY-MM-DDThh:mm:ssTZD'
            )
            return False

        current_time = datetime.datetime.now(tz=datetime.timezone.utc)

        if current_time < input_datetime_utc:
            self.put_msg("Field 'Start Time' is in the future")
            return False
        if current_time - input_datetime_utc > datetime.timedelta(
            self.max_days_in_the_past
        ):
            self.put_msg(
                f'Field "Start Time" cannot be more than {self.max_days_in_the_past} days ago',
            )
            return False

        return True


BoolValidator = validator.AnyOf(
    validator.UserDefined(
        lambda value, data, *args, **kwargs: solnlib.utils.is_true(value)
        or solnlib.utils.is_false(value)
    )
)


def retry_fn(
    f, logger, exceptions=Exception, tries=-1, delay=0, max_delay=None, backoff=1
):
    assert tries != 0, f"function='{f.__name__}' configured to be never executed."

    if tries == -1:
        logger.warning(f'message="function={f.__name__} infinite retries"')
    _tries, _delay = tries, delay
    _total_waited = delay
    while _tries:
        try:
            return f()
        except exceptions as e:
            _tries -= 1
            if not _tries:
                raise

            logger.warning(
                f'message="exception={e} next_retry_in_sec={_delay} total_waited_sec={_total_waited - _delay}"',
                exc_info=e,
            )
            time.sleep(_delay)
            _delay *= backoff

            if max_delay is not None:
                _delay = min(_delay, max_delay)
            _total_waited += _delay


def log_time_of_execution(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        logger = getattr(self, "_logger", default_logger)
        start_time = time.time()
        try:
            return func(self, *args, **kwargs)
        finally:
            end_time = time.time()
            total_time = end_time - start_time
            logger.debug(f"method={func.__name__} runtime_sec={total_time:.6f}")

    return wrapper


def empty_field(field_: field.RestField) -> field.RestField:
    return field.RestField(
        name=field_.name,
        required=False,
        encrypted=False,
        default=None,
        validator=field_.validator,
        converter=field_.converter,
    )


def find_config_in_settings(key: str, default=None, *args):
    for settings in args:
        value = settings.get(key)
        if value:
            return value

    return default


class LogLevelField(Field):
    """LogLevelField with critical status and with no default value"""

    def __init__(self, key, **kwargs):
        super(LogLevelField, self).__init__(key, **kwargs)

    def parse(self, document):
        value = super(LogLevelField, self).parse(document)
        lookup = {
            "INFO": logging.INFO,
            "DEBUG": logging.DEBUG,
            "ERROR": logging.ERROR,
            "WARNING": logging.WARNING,
            "CRITICAL": logging.CRITICAL,
        }
        return lookup.get(value)


def md5(string_to_encode: str):
    try:
        return hashlib.new(
            name="md5", data=string_to_encode.encode(), usedforsecurity=False
        )
    except ValueError:
        # Only happens on python less than 39 and FIPS enabled
        return pymd5.md5(string_to_encode.encode())


class IntervalTimer(threading.Thread):
    def __init__(self, interval, function, args=None, kwargs=None):
        self.interval = interval
        self.function = function
        self.args = args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}
        self._stopped = threading.Event()
        super().__init__()

    def run(self):
        while not self._stopped.is_set():
            self.function(*self.args, **self.kwargs)
            time.sleep(self.interval)

    def stop(self):
        self._stopped.set()
