#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#
import urllib.request, urllib.parse, urllib.error

SEPARATOR = "___"


def get_checkpoint_name(args):
    return urllib.parse.quote(SEPARATOR.join(args), "")


def get_blob_checkpoint_name(container_name, blob_name, snapshot=None):
    args = [container_name, blob_name]
    if snapshot:
        args.append(snapshot)
    return get_checkpoint_name(args)
