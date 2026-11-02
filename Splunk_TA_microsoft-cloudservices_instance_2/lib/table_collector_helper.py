#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
import contextlib
import datetime
from typing import Optional

import isodate


class Ticks(int):
    """
    https://learn.microsoft.com/en-us/dotnet/api/system.datetime.ticks?view=net-8.0&redirectedfrom=MSDN#System_DateTime_Ticks
    https://github.com/dotnet/runtime/blob/5535e31a712343a63f5d7d796cd874e563e5ac14/src/libraries/System.Private.CoreLib/src/System/DateTime.cs#L1566C30-L1566C59
    """

    _TICKS_START_TIME = datetime.datetime(
        1, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
    )
    _TICKS_PER_SECOND = 10000000

    def __new__(cls, value: int):
        assert value >= 0, "Can not be unsigned"
        return int.__new__(cls, value)

    def __str__(self):
        return f"0{super().__str__()}"

    def __add__(self, other):
        if isinstance(other, Ticks):
            return Ticks(int(self) + int(other))
        elif isinstance(other, int):
            return Ticks(int(self) + other)
        elif isinstance(other, datetime.timedelta):
            return Ticks(int(self) + int(self.from_timedelta(other)))
        return super().__add__(other)

    def __sub__(self, other):
        if isinstance(other, Ticks):
            return Ticks(int(self) - int(other))
        elif isinstance(other, int):
            return Ticks(int(self) - other)
        elif isinstance(other, datetime.timedelta):
            return Ticks(int(self) - int(self.from_timedelta(other)))
        elif isinstance(other, datetime.datetime):
            return Ticks(int(self) - int(self.from_datetime(other)))
        return super().__sub__(other)

    @classmethod
    def from_timedelta(cls, delta: datetime.timedelta) -> "Ticks":
        return cls(delta.total_seconds() * cls._TICKS_PER_SECOND)

    @classmethod
    def from_datetime(cls, dt: datetime.datetime) -> "Ticks":
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return cls((dt - cls._TICKS_START_TIME).total_seconds() * cls._TICKS_PER_SECOND)


_WAD_TABLES = [
    "WadLogsTable",
    "WADDiagnosticInfrastructureLogsTable",
    "WADDirectoriesTable",
    "WADPerformanceCountersTable",
    "WADWindowsEventLogsTable",
]


def _is_wad_table(table_name):
    upper_table_name = table_name.upper()
    return any(tb.upper() == upper_table_name for tb in _WAD_TABLES)


def is_websitesapp_table(table_name):
    return table_name.lower().startswith("websitesapplogs")


def _isodate_parse(t: str) -> Optional[datetime.datetime]:
    with contextlib.suppress(Exception):
        timestamp = isodate.parse_datetime(t)
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)
        return timestamp


def generate_partition_key(table_name, start_time, end_time):
    start_time, end_time = _isodate_parse(start_time), _isodate_parse(end_time)
    start, end = None, None

    if _is_wad_table(table_name):
        with contextlib.suppress(Exception):
            # 15 minutes earlier from now
            start = Ticks.from_datetime(start_time) - Ticks.from_timedelta(
                datetime.timedelta(minutes=15)
            )
            start = str(start)
        with contextlib.suppress(Exception):
            # Don't need offset cause the actual partition key of events in
            # range [start_time, end_time) must less than ticks count of end_time.
            end = Ticks.from_datetime(end_time)
            end = str(end)
    elif is_websitesapp_table(table_name):
        with contextlib.suppress(Exception):
            # 1 hour earlier from now
            start = (start_time - datetime.timedelta(hours=1)).strftime("%Y%m%d%H")
        with contextlib.suppress(Exception):
            end = end_time.strftime("%Y%m%d%H")
        return start, end
    # For other tables, we don't know their partition key
    # generation mechanism now.
    return start, end
