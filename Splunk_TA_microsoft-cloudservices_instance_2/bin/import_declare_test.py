#
# SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
# SPDX-License-Identifier: LicenseRef-Splunk-8-2021
#
#

import importlib.machinery
import os
import sys
import warnings

warnings.filterwarnings("ignore")

# Import the Splunk-shipped library before adding TA paths to sys.path to ensure it is cached.
# This way, even if the TA path added to sys.path contains urllib3 or requests,
# Python will always reference the already cached urllib3 or requests from the Splunk Python path.
try:
    import urllib3
except Exception as e:
    pass

try:
    import requests
except Exception as e:
    pass

# Same strategy for configparser: pre-import the stdlib version before
# prepending LIBDIR to sys.path, so a stale `lib/configparser.py` shim left
# behind by an in-place upgrade from 6.1.1 (when `configparser` was a runtime
# dependency) cannot shadow the stdlib and crash REST handlers with
# "ModuleNotFoundError: No module named 'backports.configparser'".
try:
    import configparser
except Exception as e:
    pass

PACKAGE_DIR = os.path.dirname(os.path.realpath(os.path.dirname(__file__)))
LIBDIR = os.path.join(PACKAGE_DIR, "lib")

# Filter stale future-family packages left by in-place upgrades out of import
# resolution rather than deleting them.  Older `future` does `import imp` at
# load time; `imp` was removed in Python 3.12 (PEP 594), crashing every REST
# handler on Python 3.13+ with "ModuleNotFoundError: No module named 'imp'".
# The meta-path finder below intercepts any top-level import of the four
# future-family packages and re-runs the lookup against a search path that
# excludes LIBDIR, so the stale copy is silently skipped.  If no other copy
# exists the finder raises ModuleNotFoundError (correct: future is not a
# runtime dependency).  Non-destructive: lib/ is never modified.
_STALE_FUTURE_PACKAGES = frozenset(("future", "past", "libfuturize", "libpasteurize"))
_LIBDIR_REAL = os.path.realpath(LIBDIR)


class _IgnoreStaleFutureFromLib:
    def find_spec(self, fullname, path=None, target=None):
        if path is not None:
            return None
        if fullname.partition(".")[0] not in _STALE_FUTURE_PACKAGES:
            return None
        search_path = [
            entry for entry in sys.path if os.path.realpath(entry) != _LIBDIR_REAL
        ]
        spec = importlib.machinery.PathFinder.find_spec(fullname, search_path, target)
        if spec is not None:
            return spec
        raise ModuleNotFoundError(f"No module named {fullname!r}")


sys.meta_path.insert(0, _IgnoreStaleFutureFromLib())

LIBDIR_TP_ROOT_DIR = os.path.join(LIBDIR, "3rdparty")

if sys.platform.startswith("win32"):
    PLATFORM_DIR = "windows_x86_64"
elif sys.platform.startswith("darwin"):
    PLATFORM_DIR = "darwin_x86_64"
else:
    PLATFORM_DIR = "linux_x86_64"

TPDIR = os.path.join(
    LIBDIR_TP_ROOT_DIR,
    PLATFORM_DIR,
    f"python{sys.version_info.major}{sys.version_info.minor}",
)

import_override = [TPDIR, LIBDIR]

sys.path = import_override + sys.path
