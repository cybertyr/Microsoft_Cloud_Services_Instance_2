#!/usr/bin/python
#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
from __future__ import annotations

from typing import List

from attr import define, field, validators, converters
from cattr import Converter
import mscs_consts


def validate_bool(attrib, field, value):
    if isinstance(value, bool):
        return True
    return value.lower() in (
        True,
        "true",
        "t",
        "yes",
        "y",
        "on",
        "1",
        1,
        False,
        "false",
        "f",
        "no",
        "n",
        "off",
        "0",
        0,
    )


def unsigned_int(default):
    return field(converter=int, validator=validators.gt(0), default=default)


def int_in_range(min, max, default):
    return field(
        converter=int,
        validator=[validators.ge(min), validators.le(max)],
        default=default,
    )


def boolean(default):
    return field(converter=converters.to_bool, validator=validate_bool, default=default)


def single_select(options: List, default):
    return field(converter=str, validator=validators.in_(options), default=default)


def get_converter() -> Converter:
    converter = Converter()
    converter.register_structure_hook(bool, lambda v, _: converters.to_bool(v))
    return converter


@define
class GlobalSettings:
    worker_threads_num: int = int_in_range(1, 100, 10)
    query_entities_page_size: int = int_in_range(100, 10000, 1000)
    event_cnt_per_item: int = int_in_range(10, 1000, 100)
    query_end_time_offset: int = int_in_range(20, 600, 180)
    get_blob_batch_size: int = int_in_range(10000, 1000000, 120000)
    http_timeout: int = unsigned_int(120)
    skip_files_monitoring_for_restart: bool = boolean(False)
    event_hub_watchdog_enabled: bool = boolean(True)
    load_balancing_strategy: str = single_select(
        [
            mscs_consts.LOAD_BALANCING_STRATEGY_BALANCED,
            mscs_consts.LOAD_BALANCING_STRATEGY_GREEDY,
        ],
        mscs_consts.LOAD_BALANCING_STRATEGY_BALANCED,
    )
    max_attempts: int = int_in_range(1, 100, 12)

    @classmethod
    def from_dict(cls, config: dict) -> GlobalSettings:
        if not isinstance(config, dict):
            raise ValueError("Global settings config is not a dict")

        return get_converter().structure(config, cls)
