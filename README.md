# Microsoft Cloud Services — Instance 2 (Splunk TA)

Fork of **Splunk Add-on for Microsoft Cloud Services 6.3.3** so it can run **side-by-side** with the stock `Splunk_TA_microsoft-cloudservices` app, each pointing at a different Microsoft tenant / Azure subscription set.

## Opening and extracting a `.spl` file

Splunk app packages (`.spl`) are usually **gzip-compressed tar archives** (sometimes plain `.tar` or `.zip`).

```bash
# Inspect
file splunk-add-on-for-microsoft-cloud-services_633.spl

# Extract (this build used the tarball name Splunk_TA_microsoft-cloudservices-6.3.3.tar inside gzip)
mkdir -p ../extracted
tar -xzf splunk-add-on-for-microsoft-cloud-services_633.spl -C ../extracted
# → ../extracted/Splunk_TA_microsoft-cloudservices/
```

Vendor package for this repo: `/home/cybertyr/Téléchargements/splunk-add-on-for-microsoft-cloud-services_633.spl`  
Extracted copy (not committed): `../extracted/Splunk_TA_microsoft-cloudservices/`

## Deployable app in this repository

| Item | Instance 1 (vendor) | Instance 2 (this repo) |
|------|---------------------|-------------------------|
| App folder | `Splunk_TA_microsoft-cloudservices` | `Splunk_TA_microsoft-cloudservices_instance_2/` |
| Package / `[id]` name | `Splunk_TA_microsoft-cloudservices` | `Splunk_TA_microsoft-cloudservices_instance_2` |
| UI label | Splunk Add-on for Microsoft Cloud Services | Splunk Add-on for Microsoft Cloud Services (Instance 2) |
| UCC REST root | `splunk_ta_mscs` | `splunk_ta_mscs_i2` |
| Modular input schemes | `mscs_*` | `mscs_i2_*` |
| Global settings conf | `splunk_ta_mscs_settings.conf` | `splunk_ta_mscs_i2_settings.conf` |
| KV collections | `splunk_ta_mscs_metric`, … | `splunk_ta_mscs_i2_metric`, … |

Install by copying or symlinking `Splunk_TA_microsoft-cloudservices_instance_2` into `$SPLUNK_HOME/etc/apps/` on search heads and forwarders that should use the second Microsoft Cloud connection.

## What stays shared vs isolated

**Isolated per app (safe for side-by-side):**

- App-local configuration under `local/` (accounts, inputs, passwords via `storage/passwords` scoped to this app name).
- REST namespace `/servicesNS/nobody/Splunk_TA_microsoft-cloudservices_instance_2/splunk_ta_mscs_i2/...`
- Modular input types `mscs_i2_*` (Splunk registers input *schemes* globally; duplicate `mscs_*` would clash with instance 1).
- TA log files under `$SPLUNK_HOME/var/log/splunk/splunk_ta_microsoft-cloudservices_instance_2_*` (see `default/props.conf`).

**Unchanged conf *filenames* (still valid; stanzas are app-scoped via REST):**

- `mscs_azure_accounts`, `mscs_storage_accounts`, `mscs_api_settings`, `mscs_azure_audit_inputs`, `mscs_azure_resource_inputs`.

**Python package `lib/splunk_ta_mscs/`** — left as-is (import path); runtime app name comes from directory via `mscs_common_utils.APP_NAME`.

## Rebuild from vendor tarball

After updating `../extracted/Splunk_TA_microsoft-cloudservices/` from a new `.spl`:

```bash
python3 scripts/build_instance2_addon.py
```

## Splunk deployment checklist

1. Install stock TA (instance 1) and this TA (instance 2) on the same instance.
2. Configure Azure accounts separately in each app’s **Configuration** UI.
3. Create inputs only under the matching app (input types are `mscs_i2_*` here).
4. Restart Splunk or reload the app after install: `splunk reload deploy-server` / restart splunkd as appropriate.
5. Confirm REST:  
   `curl -k -u admin:pass https://localhost:8089/servicesNS/nobody/Splunk_TA_microsoft-cloudservices_instance_2/splunk_ta_mscs_i2/splunk_ta_mscs_i2_settings`

## Recommended follow-up (not automated)

- Run Splunk’s **add-on self-test** and one modular input smoke test per `mscs_i2_*` type in a lab indexer/forwarder.
- If you use **alert action** “Stop Azure VM”, validate `mscs_i2_stop_azure_vm` after upgrade.
- Re-run `build_instance2_addon.py` after each vendor `.spl` upgrade; diff `scripts/build_instance2_addon.py` if Splunk changes UCC layout.
