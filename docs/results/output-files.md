<!--
SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
SPDX-License-Identifier: Apache-2.0
-->

# Output files and current schema

Use this page when you need artifact paths or fields for a result-processing script.

## Locations and filenames

Each `tft:` entry writes one JSON file containing its flow runs, reverse runs, and
plugin outputs. Without an output-base option, the default is
`ft-logs/YYYY-MM-DD-HH-MM-SS.json`. A test's `logs:` changes the directory; relative
paths resolve from the runner's working directory.

`-o ft-logs/smoke-` instead writes `smoke-000.json`, `smoke-001.json`, etc., using the
zero-based `tft:` index. `-o ft-logs/` uses `result-000.json` in that directory. TFT
creates needed result directories. A timestamp has only second-level precision; separate
output bases help distinguish multiple runs.

TFT also writes `YYYY-MM-DD-HH-MM-SS-RESULTS` (or `smoke-000-RESULTS`), a legacy copy
with the same serialized content as its JSON file. Prefer `.json` for new tools.
Evaluation results are already present in the primary JSON output.

## JSON structure

The format below describes the **current version**, not a versioned stable API. The
baseline generator explicitly supports skipping logs from incompatible versions. Retain
the producing TFT revision with your artifacts, and validate older files with the same
revision before building cross-version scripts.

The following excerpt is drawn from the first flow in the repository's [historical input
fixture](../../tests/input1.json), re-evaluated with an empty config. Its tool result is
shortened to the end summaries. These are recorded fixture measurements, not a new
cluster run or expected speeds for your site.

```json
{
  "tft-tests": [
    {
      "flow_test": {
        "success": true,
        "msg": null,
        "tft_metadata": {
          "tft_idx": 0,
          "test_cases_idx": 0,
          "connections_idx": 0,
          "test_case_id": "POD_TO_POD_SAME_NODE",
          "test_type": "IPERF_TCP",
          "reverse": false,
          "server": {
            "name": "worker-242",
            "pod_type": "SRIOV",
            "is_tenant": true,
            "index": 0
          },
          "client": {
            "name": "worker-242",
            "pod_type": "SRIOV",
            "is_tenant": true,
            "index": 0
          },
          "expects_blocked": false
        },
        "command": "exec -t sriov-pod-worker-242-client-5201 -- iperf3 -c 10.131.1.179 -p 5201 --json -t 10",
        "result": {
          "end": {
            "sum_sent": {
              "start": 0,
              "end": 10.000269,
              "seconds": 10.000269,
              "bytes": 48560865280,
              "bits_per_second": 38847647222.28973,
              "retransmits": 99,
              "sender": true
            },
            "sum_received": {
              "start": 0,
              "end": 10.040752,
              "seconds": 10.040752,
              "bytes": 48560865280,
              "bits_per_second": 38691018585.06216,
              "sender": true
            }
          }
        },
        "bitrate_gbps": {
          "tx": 38.848,
          "rx": 38.691
        },
        "eval_result": {
          "success": true,
          "msg": null,
          "bitrate_threshold_rx": null,
          "bitrate_threshold_tx": null
        }
      },
      "plugins": []
    }
  ]
}
```
 The root `tft-tests` list contains one item per selected case/connection/instance/
direction/target-access variant. Each item contains a `flow_test` and a `plugins` list.
An empty plugin list is normal.

## Field reference

| Field | Type / unit | Meaning |
| --- | --- | --- |
| `tft-tests` | array | All aggregate run entries for one test group. |
| `flow_test.success` | boolean | Handler's recorded traffic success; expected-block handlers can treat an unsuccessful connection as success. |
| `flow_test.msg` | string or null | Handler explanation, especially failure/expected blocking. |
| `flow_test.command` | string | Recorded client command, useful for checking destination and options. |
| `flow_test.result` | object | Tool-specific parsed/raw data; see reading-results page. |
| `flow_test.bitrate_gbps.tx`, `.rx` | number or null | Gbit/s where applicable; null means unavailable. Netperf RR uses transactions/s divided by 1000 despite the field name. |
| `flow_test.eval_result` | object or null | Final threshold/expected-block decision; null is possible in older or unevaluated data. |
| `eval_result.success` | boolean | Final flow evaluation used by the printer and aggregate checks. |
| `eval_result.msg` | string or null | Reason for evaluation failure. |
| `eval_result.bitrate_threshold_rx`, `.bitrate_threshold_tx` | number or null | Applied directional threshold; null means no threshold. |
| `flow_test.tft_metadata` | object | Case, connection, endpoint, and variant identity. |
| `tft_metadata.tft_idx` | zero-based integer | Index in `tft:`. |
| `tft_metadata.test_cases_idx` | zero-based integer | Position in the effective case union; not the case ID. |
| `tft_metadata.connections_idx` | zero-based integer | Connection position. |
| `tft_metadata.test_case_id` | enum-name string | For example `POD_TO_POD_SAME_NODE`, not the numeric ID. |
| `tft_metadata.test_type` | enum-name string | For example `IPERF_TCP`. |
| `tft_metadata.reverse` | boolean | Normal (`false`) or reverse (`true`) traffic direction. |
| `tft_metadata.target_access_mode` | enum-name string | `IP`, `SERVICE_NAME`, `CLIENT_NODE_IP`, `SERVER_NODE_IP`; default `IP` is omitted during serialization. |
| `tft_metadata.expects_blocked` | boolean | Whether a blocked traffic path is expected. Older files may omit it. |
| `tft_metadata.server`, `.client` | endpoint objects | Name, pod type, cluster role and instance index. |
| `server/client.name` | string | Endpoint **node name** in current metadata, despite the `PodInfo` type name. |
| `server/client.pod_type` | enum-name string | `NORMAL`, `HOSTBACKED`, `SRIOV`, or `SECONDARY`. |
| `server/client.is_tenant` | boolean | Tenant vs infra cluster marker. |
| `server/client.index` | integer | Instance metadata; use alongside case/connection/variant identity. |
| `plugins[]` | array | Each plugin task's recorded output. |
| `plugins[].success`, `.msg` | boolean; string or null | Independent plugin validation and explanation. |
| `plugins[].command` | string | Measurement/check command. |
| `plugins[].result` | object | Plugin-specific measurements plus command details where provided. |
| `plugins[].plugin_metadata` | object | `plugin_name`, `node_name`, `pod_name`, and `plugin_role` (possibly empty in older files). |

For a reverse subtest the metadata keeps the client/server endpoint identities;
`reverse: true` describes traffic reversal. An additional target-access variant is its
own result with its own plugin list. Do not count list length as number of unique case
IDs.

## Plugin example

This is an existing plugin output from a historical fixture, not a new measurement. It
belongs in an aggregate entry's `plugins` list:

```json
{
  "success": true,
  "msg": null,
  "command": "exec tools-pod-worker-246-measure-cpu -- mpstat -P ALL 30 1",
  "result": {
    "cpu": "all",
    "percent_usr": 0.51,
    "percent_nice": 0.0,
    "percent_sys": 0.17,
    "percent_iowait": 0.0,
    "percent_irq": 0.03,
    "percent_soft": 0.02,
    "percent_steal": 0.0,
    "percent_guest": 0.0,
    "percent_gnice": 0.0,
    "percent_idle": 99.27,
    "type": "cpu",
    "time": "19:28:52"
  },
  "plugin_metadata": {
    "plugin_name": "measure_cpu",
    "node_name": "worker-246",
    "pod_name": "tools-pod-worker-246-measure-cpu",
    "plugin_role": "server"
  }
}
```
 Refer to [plugins](../configuring/plugins.md) for which fields carry actual
measurements and which indicate skipped VF validation.

## Console and rendered manifests

`tft.py -v debug` enables detailed command and startup logging. Available levels are
`debug`, `info` (default), `warning`, `error`, and `critical`. `TFT_LOG_PREAMBLE=false`
suppresses the timestamp/thread prefix; the log level remains. The console includes
flow/plugin pass counts and a result-file path.

Rendered manifests are written to repo `manifests/yamls`, or `TFT_MANIFESTS_YAMLS` when
set. That directory must already exist. Templates in `TFT_MANIFESTS_OVERRIDES` take
precedence over bundled templates. These YAMLs are useful for inspecting ports,
attachments, security settings, resources, and runtimeClassName; rendered YAML alone
does not prove runtime success.

## Next steps

- [Read results](reading-results.md)
- [Evaluation](evaluation.md)
- [Run options](../getting-started/running-tests.md)
