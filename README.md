# Microsoft Cloud Services — Instance 2 (Splunk TA)

Minimal side-by-side fork of **Splunk Add-on for Microsoft Cloud Services 6.3.3** so it can run next to the stock `Splunk_TA_microsoft-cloudservices` app (second Microsoft tenant / Azure subscription set).

> **This repository root is not a Splunk app.**  
> Installing the GitHub ZIP / clone root (e.g. `Microsoft_Cloud_Services_Instance_2-main`) into `$SPLUNK_HOME/etc/apps/` produces the **generic Search** UI.  
> See `DO_NOT_INSTALL_THIS_REPO_ROOT.txt`.

## Install into Splunk (required)

Copy **only** the inner TA folder. The directory name under `etc/apps/` must be exactly:

`Splunk_TA_microsoft-cloudservices_instance_2`

### Option A — install script

```bash
export SPLUNK_HOME=/opt/splunk   # adjust if needed
bash scripts/install_to_splunk.sh
"$SPLUNK_HOME/bin/splunk" restart
```

### Option B — manual copy

```bash
# From a git clone of this repo:
cp -a Splunk_TA_microsoft-cloudservices_instance_2 "$SPLUNK_HOME/etc/apps/"

# Or from a GitHub ZIP extract (folder often ends with -main):
cp -a Microsoft_Cloud_Services_Instance_2-main/Splunk_TA_microsoft-cloudservices_instance_2 \
  "$SPLUNK_HOME/etc/apps/"

"$SPLUNK_HOME/bin/splunk" restart
```

### Option C — packaged tarball (recommended for sharing)

```bash
bash scripts/package_ta.sh
# → dist/Splunk_TA_microsoft-cloudservices_instance_2-YYYYMMDD.tgz
tar -xzf dist/Splunk_TA_microsoft-cloudservices_instance_2-*.tgz -C "$SPLUNK_HOME/etc/apps/"
"$SPLUNK_HOME/bin/splunk" restart
```

### Side-by-side with vendor TA

| | Vendor (Instance 1) | This repo (Instance 2) |
|--|---------------------|-------------------------|
| Install path | `$SPLUNK_HOME/etc/apps/Splunk_TA_microsoft-cloudservices/` | `$SPLUNK_HOME/etc/apps/Splunk_TA_microsoft-cloudservices_instance_2/` |
| App label | Splunk Add-on for Microsoft Cloud Services | Splunk Add-on for Microsoft Cloud Services (Instance 2) |
| Nav | Inputs / Configuration / Search / Health Dashboard | same |
| Input schemes | `mscs_*` | `mscs_i2_*` |
| UCC REST | `splunk_ta_mscs` | `splunk_ta_mscs_i2` |

Both folders can exist on the same Splunk instance. Configure accounts and inputs **inside each app** separately.

### Expected UI after a correct install

| Check | Expected |
|-------|----------|
| App label | Splunk Add-on for Microsoft Cloud Services (Instance 2) |
| Nav tabs | Inputs, Configuration, Search, Health Dashboard |
| Default page | Inputs (`…/inputs`) |
| URL app id | `/app/Splunk_TA_microsoft-cloudservices_instance_2/…` |

### If you see generic Search / Analytics / Datasets

You almost certainly installed the **repo root** (GitHub ZIP name `…-main`), not the TA folder.

1. Remove the wrong app directory (e.g. `…/Microsoft_Cloud_Services_Instance_2-main`).
2. Install `Splunk_TA_microsoft-cloudservices_instance_2` with Option A/B/C.
3. Restart Splunk and reopen the Instance 2 app.

Do **not** rename the TA folder; UCC templates hardcode `Splunk_TA_microsoft-cloudservices_instance_2`.

## Exact rename map (coexistence only)

Applied by `scripts/build_instance2_addon.py` from a fresh vendor extract:

| Kind | Vendor | Instance 2 |
|------|--------|------------|
| App folder / `package.id` / `[id]` / `globalConfig.meta.name` | `Splunk_TA_microsoft-cloudservices` | `Splunk_TA_microsoft-cloudservices_instance_2` |
| UI label / displayName | Splunk Add-on for Microsoft Cloud Services | … (Instance 2) |
| UCC `restRoot` | `splunk_ta_mscs` | `splunk_ta_mscs_i2` |
| Settings conf | `splunk_ta_mscs_settings` | `splunk_ta_mscs_i2_settings` |
| KV collections | `splunk_ta_mscs_metric`, `splunk_ta_mscs_azure_consumption` | `…_i2_metric`, `…_i2_azure_consumption` |
| Modular input schemes | `mscs_azure_event_hub`, `mscs_storage_blob`, `mscs_storage_table`, `mscs_azure_audit`, `mscs_azure_resource`, `mscs_azure_metrics`, `mscs_azure_kql`, `mscs_azure_consumption` | same with `mscs_i2_` prefix |
| Alert action | `mscs_stop_azure_vm` | `mscs_i2_stop_azure_vm` |
| TA log source prefix (`props.conf`) | `splunk_ta_microsoft-cloudservices_*` | `splunk_ta_microsoft-cloudservices_instance_2_*` |
| UCC input REST endpoints | `{restRoot}_{scheme}` e.g. `splunk_ta_mscs_mscs_storage_blob` | e.g. `splunk_ta_mscs_i2_mscs_i2_storage_blob` |
| `check_for_updates` | `true` | `false` |

**Not renamed (on purpose):**

- Python package `lib/splunk_ta_mscs/` and `from/import splunk_ta_mscs…`
- Account / API conf files: `mscs_azure_accounts`, `mscs_storage_accounts`, `mscs_api_settings`, `mscs_azure_*_inputs`
- Data sourcetypes (`mscs:azure:*`, `mscs:storage:*`, …)
- Third-party libraries under `lib/`

## Rebuild from vendor `.spl`

```bash
mkdir -p ../extracted
rm -rf ../extracted/Splunk_TA_microsoft-cloudservices
tar -xzf /path/to/splunk-add-on-for-microsoft-cloud-services_633.spl -C ../extracted
python3 scripts/build_instance2_addon.py
# optional: python3 scripts/build_instance2_addon.py --src /other/path/Splunk_TA_microsoft-cloudservices
```

The script is re-runnable: it deletes and recreates `Splunk_TA_microsoft-cloudservices_instance_2/` from the vendor tree, then applies only the renames above.

## Verification (no live Splunk required)

Already covered by rebuilding + checks in this repo workflow:

- Diff vs vendor: only coexistence paths change; `lib/splunk_ta_mscs/` and third-party libs untouched
- No broken imports (`splunk_ta_mscs_i2`, `rest_handlers.mscs_i2_*`, …)
- INI parse: `app.conf`, `inputs.conf`, `restmap.conf`, `web.conf`, `collections.conf`, settings
- `nav/default.xml` still has Inputs / Configuration / Search / Health
- `globalConfig.json` `meta.name` = app folder; `restRoot` matches restmap
- Modular input `Scheme()` / `DataInputModel` strings use `mscs_i2_*`; bin scripts keep imports of unrenamed lib modules
- Every `handlerfile=` / `script=` in restmap exists under `bin/`

### Deploy smoke tests (on a Splunk host)

1. Install vendor TA + this TA under the two folder names above; restart.
2. Open Instance 2 → expect UCC Inputs (not generic Search).
3. Add one Azure account under Instance 2 Configuration (does not appear in vendor app).
4. Create one disabled input of type `mscs_i2_azure_event_hub` (or any `mscs_i2_*`).
5. Optional REST check:  
   `…/servicesNS/nobody/Splunk_TA_microsoft-cloudservices_instance_2/splunk_ta_mscs_i2/splunk_ta_mscs_i2_settings`
6. If using alert “Stop Azure VM”, validate action `mscs_i2_stop_azure_vm`.
