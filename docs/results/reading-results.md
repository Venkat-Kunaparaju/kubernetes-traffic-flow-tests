<!--
SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
SPDX-License-Identifier: Apache-2.0
-->

# Read and compare results

Use this page when you have result JSON and need to interpret the measurements.

## Print saved results

```bash
./print_results.py --no-color ft-logs/run-000.json
./print_results.py --no-color ft-logs/run-a-000.json ft-logs/run-b-000.json
```
 The command accepts one or more JSON inputs. `--no-color` disables color; `NO_COLOR`
does the same. Color otherwise appears only on a terminal. `-v` / `--verbose` enables
debug logging. Exit code is `0` when all files pass, `1` when any recorded flow/plugin
fails; invalid input can raise a separate error.

The printer groups entries under Passing Flows and Failing Flows. A failing plugin
places its aggregate entry in Failing Flows even if the flow line itself says
`succeeded`. The summary is not a fresh measurement or re-evaluation; it uses the
evaluation recorded in each file.

For a concrete offline example, use an existing historical fixture:

```bash
./evaluator.py '' tests/input1.json /tmp/tft-fixture-evaluated.json
./print_results.py --no-color /tmp/tft-fixture-evaluated.json
```

```text
Test ID: (1) POD_TO_POD_SAME_NODE, Test Type: IPERF_TCP, Reverse: false, TX Bitrate: 38.848 Gbps, RX Bitrate: 38.691 Gbps, succeeded
Test ID: (1) POD_TO_POD_SAME_NODE, Test Type: IPERF_TCP, Reverse: true, TX Bitrate: 42.193 Gbps, RX Bitrate: 42.364 Gbps, succeeded
```

This excerpt shows the first two passing entries produced from the fixture by
the current tools. The complete fixture also contains an intentional ANP-deny
failure: the printer returns `1` for that full file, demonstrating the failure
path. `Test ID` is the numeric ID and enum name; `Reverse` identifies direction.
`Target Access` is printed only when it differs from the current default for that case. The JSON metadata records
the actual access mode, except default `IP` is omitted.

## Tool metrics

| Tool | Where to look | Interpretation |
| --- | --- | --- |
| iperf TCP | `flow_test.bitrate_gbps`, `result.end.sum_sent`, `result.end.sum_received` | TX/RX normalized Gbit/s; raw `bits_per_second` is bit/s. Retransmits, when present, are packet counts in sender data. Direction and route matter when comparing runs. |
| iperf UDP | `result.end.sum` | `jitter_ms` is ms; `lost_packets` and `packets` are counts; `lost_percent` is percent. This handler populates both normalized directions from that sum rate. Check offered bandwidth as well as achieved rate. |
| netperf TCP stream | `result["Throughput 10^6bits/sec"]` | Raw Mbit/s; normalized TX divides by 1000. RX is null. |
| netperf TCP RR | `result["Transaction Rate Per Second"]` | Transactions/s. The current normalized TX divides by 1000, so the printer's Gbps label is misleading for this type. |
| HTTP | `success`, `eval_result`, `result.result` | Command status and expected-body validation; no throughput metric. Deny cases invert the expected connection outcome. |
| RDMA | `result.output`, `result.bandwidth_gbps` | Average perftest Gbit/s; raw output includes message rate in Mpps. Verify actual device/data path. |
| simple | `success`, `result.result` | TCP script or custom-script outcome, no normalized bitrate. |

A null rate is unavailable, not automatically zero. It is normal for HTTP, simple, some
tool directions, and blocked traffic. Missing thresholds do not waive a tool failure on
an ordinary allow case.

## Plugin output

`measure_cpu` reports `percent_idle`: approximate busy percentage is `100 -
percent_idle`. The current plugin stores the parsed aggregate CPU row; it is not a
per-pod CPU usage measurement. In DPU mode it samples paired DPU nodes. `measure_power`
reports average sampled watts as a string, not energy. Neither plugin enforces a
utilization or power budget through evaluation YAML.

Offload plugins report their statistics backend, command results, and RX/TX start/end
counts. Subtract start from end and inspect the message for skips: host-backed endpoints
and external servers have no VF representor to validate. `ping_mgmt_port` records its
target management IP, server node, and ping result. A successful traffic flow and failed
plugin still fail the aggregate result.

## Compare runs with jq

Select identity and rates; default missing target mode to `IP`:

```bash
jq -r '."tft-tests"[] | .flow_test | [.tft_metadata.test_case_id, .tft_metadata.test_type, .tft_metadata.connections_idx, .tft_metadata.client.index, .tft_metadata.reverse, (.tft_metadata.target_access_mode // "IP"), .bitrate_gbps.tx, .bitrate_gbps.rx] | @tsv' ft-logs/run-000.json
```
 List threshold failures and plugin failures:

```bash
jq '."tft-tests"[] | select(.flow_test.eval_result.success == false or any(.plugins[]; .success == false))' ft-logs/run-000.json
```
 Produce sorted comparable views of two runs:

```bash
jq -S '[."tft-tests"[] | {metadata: .flow_test.tft_metadata, bitrate: .flow_test.bitrate_gbps}] | sort_by(.metadata | tostring)' ft-logs/run-a-000.json > /tmp/tft-a.json
jq -S '[."tft-tests"[] | {metadata: .flow_test.tft_metadata, bitrate: .flow_test.bitrate_gbps}] | sort_by(.metadata | tostring)' ft-logs/run-b-000.json > /tmp/tft-b.json
diff -u /tmp/tft-a.json /tmp/tft-b.json
```
 Keep node names, selected cases, connection order, instance counts, runtime, resource
limits, tool arguments, images, and target-access settings consistent. Those differences
can explain rate changes without a networking regression.

## Interpretation tips

Same-node vs different-node differences help identify effects of the cross-node path.
Pod vs host-network comparisons change endpoint networking and are not identical
measurements. Use the matching direction and access mode when comparing Services. For
deny/isolation cases, “succeeded” means traffic was blocked as expected; do not judge
them using a positive-throughput baseline.

## Next steps

- [Evaluation thresholds](evaluation.md)
- [Create baselines](baselines.md)
- [Output schema](output-files.md)
