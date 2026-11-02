#!/usr/bin/env python3
"""
Build Splunk_TA_microsoft-cloudservices_instance_2 from vendor MSCS 6.3.3.

Minimal coexistence transforms only — see RENAME_MAP below and README.md.

Usage (from repo root or scripts/):
  python3 scripts/build_instance2_addon.py
  python3 scripts/build_instance2_addon.py --src /path/to/Splunk_TA_microsoft-cloudservices

Expects a fresh vendor extract at:
  ../extracted/Splunk_TA_microsoft-cloudservices/
(or pass --src). Re-extract with:
  tar -xzf /path/to/splunk-add-on-for-microsoft-cloud-services_633.spl \\
      -C ../extracted

================================================================================
RENAME MAP (exact)
================================================================================
App folder / package id / [id] name / globalConfig meta.name
  Splunk_TA_microsoft-cloudservices
  → Splunk_TA_microsoft-cloudservices_instance_2

UI label / launcher description / manifest description / displayName
  Splunk Add-on for Microsoft Cloud Services
  → Splunk Add-on for Microsoft Cloud Services (Instance 2)

UCC restRoot (+ derived REST stanza/endpoint/expose prefixes)
  splunk_ta_mscs → splunk_ta_mscs_i2
  (Python package imports `from/import splunk_ta_mscs…` are preserved)

Global settings conf (+ server.conf / app.conf reload / schema refs)
  splunk_ta_mscs_settings[.conf]
  → splunk_ta_mscs_i2_settings[.conf]

KV collections (keep code/conf in sync; collections are referenced by name)
  splunk_ta_mscs_metric → splunk_ta_mscs_i2_metric
  splunk_ta_mscs_azure_consumption → splunk_ta_mscs_i2_azure_consumption

Modular input schemes (+ bin script basenames, RH handlerfiles for inputs,
inputs.conf / globalConfig service names / DataInputModel / Scheme() strings)
  mscs_azure_consumption → mscs_i2_azure_consumption
  mscs_azure_event_hub   → mscs_i2_azure_event_hub
  mscs_azure_metrics     → mscs_i2_azure_metrics
  mscs_storage_table     → mscs_i2_storage_table
  mscs_storage_blob      → mscs_i2_storage_blob
  mscs_azure_kql         → mscs_i2_azure_kql
  mscs_azure_audit       → mscs_i2_azure_audit   (NOT mscs_azure_audit_inputs)
  mscs_azure_resource    → mscs_i2_azure_resource (NOT mscs_azure_resource_inputs)

Alert action
  mscs_stop_azure_vm → mscs_i2_stop_azure_vm

TA log source prefix in props.conf (app-folder-based log names)
  splunk_ta_microsoft-cloudservices
  → splunk_ta_microsoft-cloudservices_instance_2

package check_for_updates
  true → false

NOT renamed (intentionally):
  - Python package dir lib/splunk_ta_mscs/ and `from/import splunk_ta_mscs…`
  - Account/storage conf files: mscs_azure_accounts, mscs_storage_accounts,
    mscs_api_settings, mscs_azure_*_inputs
  - Data sourcetypes (mscs:azure:*, mscs:storage:*, …)
  - Third-party libs under lib/
================================================================================
"""
from __future__ import annotations

import argparse
import os
import re
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SRC = REPO_ROOT.parent / "extracted" / "Splunk_TA_microsoft-cloudservices"
DST = REPO_ROOT / "Splunk_TA_microsoft-cloudservices_instance_2"

OLD_APP = "Splunk_TA_microsoft-cloudservices"
NEW_APP = "Splunk_TA_microsoft-cloudservices_instance_2"
OLD_LOG_PREFIX = "splunk_ta_microsoft-cloudservices"
NEW_LOG_PREFIX = "splunk_ta_microsoft-cloudservices_instance_2"
OLD_REST = "splunk_ta_mscs"
NEW_REST = "splunk_ta_mscs_i2"
OLD_LABEL = "Splunk Add-on for Microsoft Cloud Services"
NEW_LABEL = "Splunk Add-on for Microsoft Cloud Services (Instance 2)"

# Longest-first; negative lookahead keeps *_inputs conf types intact.
MODINPUT_REGEX: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bmscs_azure_consumption\b"), "mscs_i2_azure_consumption"),
    (re.compile(r"\bmscs_azure_event_hub\b"), "mscs_i2_azure_event_hub"),
    (re.compile(r"\bmscs_azure_metrics\b"), "mscs_i2_azure_metrics"),
    (re.compile(r"\bmscs_storage_table\b"), "mscs_i2_storage_table"),
    (re.compile(r"\bmscs_storage_blob\b"), "mscs_i2_storage_blob"),
    (re.compile(r"\bmscs_azure_kql\b"), "mscs_i2_azure_kql"),
    (re.compile(r"\bmscs_azure_audit\b(?!_inputs)"), "mscs_i2_azure_audit"),
    (re.compile(r"\bmscs_azure_resource\b(?!_inputs)"), "mscs_i2_azure_resource"),
    (re.compile(r"\bmscs_stop_azure_vm\b"), "mscs_i2_stop_azure_vm"),
]

MODINPUT_PAIRS = [
    ("mscs_azure_consumption", "mscs_i2_azure_consumption"),
    ("mscs_azure_event_hub", "mscs_i2_azure_event_hub"),
    ("mscs_azure_metrics", "mscs_i2_azure_metrics"),
    ("mscs_storage_table", "mscs_i2_storage_table"),
    ("mscs_storage_blob", "mscs_i2_storage_blob"),
    ("mscs_azure_kql", "mscs_i2_azure_kql"),
    ("mscs_azure_audit", "mscs_i2_azure_audit"),
    ("mscs_azure_resource", "mscs_i2_azure_resource"),
    ("mscs_stop_azure_vm", "mscs_i2_stop_azure_vm"),
]

# Do not rewrite Python package sources (imports stay splunk_ta_mscs).
SKIP_CONTENT_PREFIXES = (
    f"lib{os.sep}splunk_ta_mscs{os.sep}",
)

TEXT_SUFFIXES = {
    ".conf",
    ".py",
    ".json",
    ".xml",
    ".html",
    ".js",
    ".txt",
    ".spec",
    ".manifest",
    ".md",
    ".csv",
}

# Protect `from splunk_ta_mscs…` / `import splunk_ta_mscs…` during restRoot rewrite.
IMPORT_PROTECT = re.compile(
    r"\b((?:from|import)\s+)splunk_ta_mscs\b"
)

# After restRoot rewrite, restore RH *filenames* (on-disk scripts keep splunk_ta_mscs_rh_*).
HANDLERFILE_FIX = re.compile(
    r"(?m)^(\s*(?:handlerfile|script)\s*=\s*)splunk_ta_mscs_i2_rh_"
)
HANDLER_CLASS_FIX = re.compile(
    r"(?m)^(\s*handler\s*=\s*)splunk_ta_mscs_i2_rh_"
)


def should_transform_content(path: Path) -> bool:
    if path.suffix in TEXT_SUFFIXES or path.name in ("app.manifest", "THIRDPARTY", "VERSION"):
        return True
    return False


# Python modules we keep at vendor filenames (do not follow scheme rename in imports).
_KEEP_MODULE_PREFIXES = (
    "rest_handlers.",
    "modular_inputs.",
    "modular_inputs.scripts.",
    "modular_inputs.checkpoints.",
)


def apply_modinput_renames(text: str) -> str:
    """Rename scheme identifiers; keep imports pointing at unrenamed lib modules."""
    for pattern, repl in MODINPUT_REGEX:
        text = pattern.sub(repl, text)

    # RH script basenames embed the scheme after an underscore (no \b before mscs_).
    for old, new in MODINPUT_PAIRS:
        text = text.replace(f"splunk_ta_mscs_rh_{old}.py", f"splunk_ta_mscs_rh_{new}.py")

    # Restore imports of lib modules we intentionally did not rename on disk.
    for old, new in MODINPUT_PAIRS:
        for prefix in _KEEP_MODULE_PREFIXES:
            text = text.replace(f"from {prefix}{new}", f"from {prefix}{old}")
            text = text.replace(f"import {prefix}{new}", f"import {prefix}{old}")
    return text


def apply_rest_endpoint_scheme_suffixes(text: str, rest_prefix: str) -> str:
    """UCC endpoints are `{restRoot}_{scheme}`; scheme sits after `_` so \\b misses it."""
    for old, new in MODINPUT_PAIRS:
        text = text.replace(f"{rest_prefix}_{old}", f"{rest_prefix}_{new}")
    return text


def apply_rest_root(text: str) -> str:
    """Rename restRoot / derived identifiers; keep Python package imports."""
    protected: list[str] = []

    def _protect(match: re.Match[str]) -> str:
        protected.append(match.group(0))
        return f"@@IMPORT_PROTECT_{len(protected) - 1}@@"

    text = IMPORT_PROTECT.sub(_protect, text)
    text = text.replace(OLD_REST, NEW_REST)
    for i, original in enumerate(protected):
        text = text.replace(f"@@IMPORT_PROTECT_{i}@@", original)

    # handlerfile/script/handler class module names stay on disk as splunk_ta_mscs_rh_*.
    # (restRoot rewrite turns splunk_ta_mscs_rh_ → splunk_ta_mscs_i2_rh_)
    text = HANDLERFILE_FIX.sub(r"\1splunk_ta_mscs_rh_", text)
    text = HANDLER_CLASS_FIX.sub(r"\1splunk_ta_mscs_rh_", text)
    return text


def apply_replacements(text: str) -> str:
    # App id before log-prefix (log prefix is lowercase / hyphenated differently).
    text = text.replace(OLD_APP, NEW_APP)
    text = text.replace(OLD_LOG_PREFIX, NEW_LOG_PREFIX)
    text = text.replace(OLD_LABEL, NEW_LABEL)
    # Schemes first (inputs.conf / Scheme() / globalConfig service names).
    text = apply_modinput_renames(text)
    # Then restRoot; then fix `{restRoot}_{oldScheme}` leftovers that \\b missed.
    text = apply_rest_root(text)
    text = apply_rest_endpoint_scheme_suffixes(text, NEW_REST)
    text = text.replace("check_for_updates = true", "check_for_updates = false")
    return text


def rename_paths(root: Path) -> None:
    """Rename only paths Splunk / UCC resolve by basename."""
    settings = root / "default" / "splunk_ta_mscs_settings.conf"
    if settings.is_file():
        settings.rename(root / "default" / "splunk_ta_mscs_i2_settings.conf")

    settings_spec = root / "README" / "splunk_ta_mscs_settings.conf.spec"
    if settings_spec.is_file():
        settings_spec.rename(root / "README" / "splunk_ta_mscs_i2_settings.conf.spec")

    for old, new in MODINPUT_PAIRS:
        mod_bin = root / "bin" / f"{old}.py"
        if mod_bin.is_file():
            mod_bin.rename(root / "bin" / f"{new}.py")

        # Input RH scripts include the scheme in the filename (restmap handlerfile).
        if old != "mscs_stop_azure_vm":
            rh = root / "bin" / f"splunk_ta_mscs_rh_{old}.py"
            if rh.is_file():
                rh.rename(root / "bin" / f"splunk_ta_mscs_rh_{new}.py")

    alert_html = root / "default" / "data" / "ui" / "alerts" / "mscs_stop_azure_vm.html"
    if alert_html.is_file():
        alert_html.rename(alert_html.with_name("mscs_i2_stop_azure_vm.html"))

    alert_helper = root / "bin" / "splunk_ta_mscs" / "modalert_mscs_stop_azure_vm_helper.py"
    if alert_helper.is_file():
        alert_helper.rename(
            alert_helper.with_name("modalert_mscs_i2_stop_azure_vm_helper.py")
        )


def transform_tree(root: Path) -> None:
    for path in sorted(root.rglob("*")):
        if not path.is_file() or not should_transform_content(path):
            continue
        rel = path.relative_to(root).as_posix().replace("/", os.sep)
        if any(rel.startswith(prefix) for prefix in SKIP_CONTENT_PREFIXES):
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        updated = apply_replacements(text)
        if updated != text:
            path.write_text(updated, encoding="utf-8")


def verify_imports(root: Path) -> list[str]:
    """Return paths with imports that point at modules we did not create/rename."""
    bad: list[str] = []
    broken = re.compile(
        r"\b(?:from|import)\s+(?:"
        r"splunk_ta_mscs_i2\b|"
        r"rest_handlers\.mscs_i2_|"
        r"modular_inputs(?:\.scripts|\.checkpoints)?\.mscs_i2_"
        r")"
    )
    for path in root.rglob("*.py"):
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        if broken.search(text):
            bad.append(path.relative_to(root).as_posix())
    return bad


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("Usage", 1)[0].strip())
    parser.add_argument(
        "--src",
        type=Path,
        default=DEFAULT_SRC,
        help=f"Vendor TA root (default: {DEFAULT_SRC})",
    )
    args = parser.parse_args()
    src: Path = args.src.resolve()

    if not (src / "default" / "app.conf").is_file():
        print(f"Source not found or incomplete: {src}", file=sys.stderr)
        return 1

    if DST.exists():
        shutil.rmtree(DST)
    shutil.copytree(src, DST)

    # Paths first, then content (so content rewrite sees final filenames only via text).
    rename_paths(DST)
    transform_tree(DST)

    bad = verify_imports(DST)
    if bad:
        print("ERROR: broken Python imports of non-existent package splunk_ta_mscs_i2:", file=sys.stderr)
        for rel in bad:
            print(f"  {rel}", file=sys.stderr)
        return 2

    # Sanity: settings conf renamed, package id updated.
    if not (DST / "default" / "splunk_ta_mscs_i2_settings.conf").is_file():
        print("ERROR: missing renamed settings conf", file=sys.stderr)
        return 2
    app_conf = (DST / "default" / "app.conf").read_text(encoding="utf-8")
    if f"id = {NEW_APP}" not in app_conf:
        print("ERROR: package id not updated in app.conf", file=sys.stderr)
        return 2
    if "from splunk_ta_mscs" not in (DST / "lib" / "mscs_util.py").read_text(encoding="utf-8"):
        print("ERROR: mscs_util.py lost splunk_ta_mscs package import", file=sys.stderr)
        return 2

    print(f"Built Instance 2 add-on at {DST}")
    print("Rename map: see docstring at top of scripts/build_instance2_addon.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
