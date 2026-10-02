<!--
SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
SPDX-License-Identifier: Apache-2.0
-->

# Add plugins

Use this page when you need CPU, power, offload, or management-port evidence alongside traffic.

Plugins run alongside a selected connection's traffic tasks. A plugin can emit one
result per endpoint; the summary counts plugin results rather than plugin names. Failure
of any enabled plugin makes the aggregate flow fail, even when traffic itself succeeds.
Evaluation YAML does not supply CPU/power thresholds: these plugins report command/parse
success, while offload and ping have their own checks.

Plugin tool pods use host networking, run privileged as root, and mount the host
filesystem. The cluster must permit these pods. Traffic-pod privilege settings do not
make plugin pods unprivileged. An infra kubeconfig changes where applicable plugins run;
it does not enable plugins automatically.

| Plugin | Question | Requirements | Output and pass/fail |
| --- | --- | --- | --- |
| `measure_cpu` | How busy were the endpoint nodes? | `mpstat` in tool image; no OVN dependency in single-cluster mode. In DPU mode, infra access and host-pairing label. | `percent_idle` and parsed CPU data plus command result. Fails on command/parse errors; no CPU usage limit. DPU mode measures paired DPU nodes. |
| `measure_power` | What was average sampled node power? | Working IPMI/DCMI power readings on endpoint nodes; no OVN API dependency. | `measure_power` string in watts from repeated `ipmitool dcmi power reading` samples. Fails if commands or readings cannot be parsed; no watt threshold. |
| `validate_offload` | Did VF representor software traffic remain below the allowed count? | OVN/SR-IOV offload deployment, discoverable VF/PF and representor, `devlink`, `ethtool`; infra pairing in DPU mode. | Start/end RX/TX software packet counts and command results. Fails if either delta is `>= 1000` packets in this upstream version, or discovery/statistics fail. |
| `ovs_doca_validate_offload` | Did OVS-DOCA representor counters meet the offload check? | OVS-DOCA deployment and accessible host-mounted OVSDB, plus VF discovery prerequisites. | Same offload check using OVS `sw_rx_packets` and `tx_packets`; backend is recorded. |
| `ping_mgmt_port` | Can the client node reach the server's OVN management port? | OVN-Kubernetes `k8s.ovn.org/node-subnets` annotation and reachable management IP. | `mgmt_port_ip`, `server_node`, and ping command result; fails if discovery or ping fails. Sends five pings with a two-second per-ping timeout. |

The offload threshold is a fixed constant in this upstream tree, not an available
`TFT_VF_REP_TRAFFIC_THRESHOLD` environment option. Host-backed endpoints and external
servers do not have a VF to validate; their plugin results can succeed with an
explanatory message and no representor statistics. Inspect endpoint role and message
before interpreting a successful plugin as proof of offload.

## Configuration

Place `plugins` under a connection. A string enables a plugin for every selected case. A
mapping can restrict it to certain cases:

```yaml
plugins:
  - name: measure_cpu
    test_cases: [POD_TO_POD_SAME_NODE, POD_TO_POD_DIFF_NODE]
  - name: measure_power
    test_cases: [POD_TO_POD_DIFF_NODE]
  - name: validate_offload
    test_cases: [POD_TO_POD_DIFF_NODE]
  - name: ovs_doca_validate_offload
    test_cases: [POD_TO_POD_DIFF_NODE]
  - name: ping_mgmt_port
    test_cases: [POD_TO_POD_DIFF_NODE]
```
 Choose the offload backend that matches your deployment; usually enable one of the two
offload plugins. The example shows the accepted configuration for each plugin and is not
a recommendation to enable all plugins on every cluster. `plugins: [measure_cpu]` is the
shorthand. `test_cases: []` disables that plugin; omitted/null selection applies it to
all selected cases.

Read `plugins[].plugin_metadata` to associate an output with its node, pod, and
server/client role. A traffic threshold failure and a plugin failure are separate
checks; re-evaluation preserves each plugin's recorded success/failure.

## Next steps

- [DPU setup and pairing](scenarios/dpu-mode.md)
- [Interpret output](../results/reading-results.md)
- [Test selection](test-cases.md#selecting-test-cases)
