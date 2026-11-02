#!/usr/bin/env python3
"""
Build Splunk_TA_microsoft-cloudservices_instance_2 from the vendor 6.3.3 package.

Run from repo root after extracting the .spl tarball to ../extracted/Splunk_TA_microsoft-cloudservices/
"""
from __future__ import annotations

import os
import re
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC = REPO_ROOT.parent / "extracted" / "Splunk_TA_microsoft-cloudservices"
DST = REPO_ROOT / "Splunk_TA_microsoft-cloudservices_instance_2"

OLD_APP = "Splunk_TA_microsoft-cloudservices"
NEW_APP = "Splunk_TA_microsoft-cloudservices_instance_2"
OLD_LOG_PREFIX = "splunk_ta_microsoft-cloudservices"
NEW_LOG_PREFIX = "splunk_ta_microsoft-cloudservices_instance_2"
OLD_REST = "splunk_ta_mscs"
NEW_REST = "splunk_ta_mscs_i2"
OLD_LABEL = "Splunk Add-on for Microsoft Cloud Services"
NEW_LABEL = "Splunk Add-on for Microsoft Cloud Services (Instance 2)"

# Modular input *types* only (not mscs_azure_audit_inputs / mscs_azure_resource_inputs conf files).
MODINPUT_REGEX: list[tuple[str, str]] = [
    (r"\bmscs_azure_consumption\b", "mscs_i2_azure_consumption"),
    (r"\bmscs_azure_event_hub\b", "mscs_i2_azure_event_hub"),
    (r"\bmscs_azure_metrics\b", "mscs_i2_azure_metrics"),
    (r"\bmscs_storage_table\b", "mscs_i2_storage_table"),
    (r"\bmscs_storage_blob\b", "mscs_i2_storage_blob"),
    (r"\bmscs_azure_kql\b", "mscs_i2_azure_kql"),
    (r"\bmscs_azure_audit\b(?!_inputs)", "mscs_i2_azure_audit"),
    (r"\bmscs_azure_resource\b(?!_inputs)", "mscs_i2_azure_resource"),
    (r"\bmscs_stop_azure_vm\b", "mscs_i2_stop_azure_vm"),
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

SKIP_REST_SUBPATHS = (
    f"lib{os.sep}splunk_ta_mscs{os.sep}",
    f"bin{os.sep}splunk_ta_mscs{os.sep}",
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
}


def should_transform_content(path: Path) -> bool:
    if path.suffix in TEXT_SUFFIXES or path.name in ("app.manifest", "THIRDPARTY", "VERSION"):
        return True
    return False


def apply_modinput_renames(text: str) -> str:
    for pattern, repl in MODINPUT_REGEX:
        text = re.sub(pattern, repl, text)
    for old, new in MODINPUT_PAIRS:
        text = text.replace(f"splunk_ta_mscs_rh_{old}.py", f"splunk_ta_mscs_rh_{new}.py")
        text = text.replace(f"{old}.py", f"{new}.py")
    return text


def apply_rest_endpoint_renames(text: str, rest_prefix: str) -> str:
    """UCC REST stanzas use `{restRoot}_{modinputType}` (e.g. splunk_ta_mscs_mscs_storage_table)."""
    for old, new in MODINPUT_PAIRS:
        text = text.replace(f"{rest_prefix}_{old}", f"{rest_prefix}_{new}")
    return text


def apply_replacements(text: str, rel_posix: str) -> str:
    text = text.replace(OLD_APP, NEW_APP)
    text = text.replace(OLD_LOG_PREFIX, NEW_LOG_PREFIX)
    text = text.replace(OLD_LABEL, NEW_LABEL)
    text = apply_rest_endpoint_renames(text, OLD_REST)
    text = apply_modinput_renames(text)
    skip_rest = any(part in rel_posix for part in SKIP_REST_SUBPATHS)
    if not skip_rest:
        text = text.replace(OLD_REST, NEW_REST)
        text = apply_rest_endpoint_renames(text, NEW_REST)
        # REST stanza prefix only — keep existing rh script filenames on disk.
        text = text.replace("splunk_ta_mscs_i2_rh", "splunk_ta_mscs_rh")
        text = text.replace(
            "splunk_ta_mscs_rh_check_account_configuration",
            "splunk_ta_mscs_i2_rh_check_account_configuration",
        )
    text = text.replace("check_for_updates = true", "check_for_updates = false")
    return text


def rename_paths(root: Path) -> None:
    settings = root / "default" / "splunk_ta_mscs_settings.conf"
    if settings.is_file():
        settings.rename(root / "default" / "splunk_ta_mscs_i2_settings.conf")

    for old, new in MODINPUT_PAIRS:
        mod_bin = root / "bin" / f"{old}.py"
        if mod_bin.is_file():
            mod_bin.rename(root / "bin" / f"{new}.py")
        rh = root / "bin" / f"splunk_ta_mscs_rh_{old}.py"
        if rh.is_file():
            rh.rename(root / "bin" / f"splunk_ta_mscs_rh_{new}.py")

    checkpoint = root / "lib" / "modular_inputs" / "checkpoints" / "mscs_azure_event_hub.py"
    if checkpoint.is_file():
        checkpoint.rename(checkpoint.with_name("mscs_i2_azure_event_hub.py"))

    mod_eh = root / "lib" / "modular_inputs" / "mscs_azure_event_hub.py"
    if mod_eh.is_file():
        mod_eh.rename(mod_eh.with_name("mscs_i2_azure_event_hub.py"))

    alert_helper = root / "bin" / "splunk_ta_mscs" / "modalert_mscs_stop_azure_vm_helper.py"
    if alert_helper.is_file():
        alert_helper.rename(
            alert_helper.with_name("modalert_mscs_i2_stop_azure_vm_helper.py")
        )


def patch_imports(root: Path) -> None:
    replacements = [
        (
            "from modular_inputs.mscs_azure_event_hub import",
            "from modular_inputs.mscs_i2_azure_event_hub import",
        ),
        (
            "import modular_inputs.checkpoints.mscs_azure_event_hub as",
            "import modular_inputs.checkpoints.mscs_i2_azure_event_hub as",
        ),
    ]
    for path in root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        new = text
        for old, new_s in replacements:
            new = new.replace(old, new_s)
        if new != text:
            path.write_text(new, encoding="utf-8")


def transform_tree(root: Path) -> None:
    for path in sorted(root.rglob("*")):
        if not path.is_file() or not should_transform_content(path):
            continue
        rel = path.relative_to(root).as_posix()
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        updated = apply_replacements(text, rel)
        if updated != text:
            path.write_text(updated, encoding="utf-8")


def main() -> int:
    if not SRC.is_dir():
        print(f"Source not found: {SRC}", file=sys.stderr)
        return 1
    if DST.exists():
        shutil.rmtree(DST)
    shutil.copytree(SRC, DST)
    rename_paths(DST)
    transform_tree(DST)
    patch_imports(DST)
    print(f"Built instance 2 add-on at {DST}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
