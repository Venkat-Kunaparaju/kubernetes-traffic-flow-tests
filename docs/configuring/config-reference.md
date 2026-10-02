<!--
SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
SPDX-License-Identifier: Apache-2.0
-->

# Configuration reference

Use this page when you need exact fields, defaults, or override rules.

This reference describes accepted YAML fields in [`testConfig.py`](../../testConfig.py).
Names and values are literal; TFT does not interpolate environment variables inside
YAML. Use one server and one client per connection. The parser accepts integer and
boolean scalar strings as well as native YAML values; prefer native scalars in new
configurations.

## Top level

| Field | Type | Required | Default | Allowed values | Description | Environment | Example |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `tft` | list | yes | — | non-empty list | One or more test groups. | — | See quickstart. |
| `kubeconfig` | string | unless supplied elsewhere | auto-detect | non-empty file path | Tenant/single-cluster kubeconfig; relative to this YAML file. | `TFT_KUBECONFIG` | `/path/tenant.yaml` |
| `kubeconfig_infra` | string | for DPU mode | unset | non-empty file path | Requires tenant kubeconfig in the same source. | `TFT_KUBECONFIG_INFRA` | `/path/infra.yaml` |
| `dpu_node_host_label` | string | for DPU pairing plugins | unset | label key | Infra-node label identifying its tenant worker. | — | `provisioning.dpu.nvidia.com/host` |

## `tft[]`

| Field | Type | Required | Default | Allowed values | Description | Environment | Example |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `name` | string | no | `Test N` | non-empty string | Name used in logs; N starts at 1. | — | `smoke` |
| `namespace` | string | no | `default` | Kubernetes namespace | Use a dedicated namespace. | — | `tft-docs` |
| `test_cases` | selection | no | all | IDs, names, ranges, list, `"*"` | Cases shared by every connection; `[]` selects none. | — | `"1,2"` |
| `duration` | integer | no | 3600 | non-negative seconds | Zero also becomes 3600; set short runs explicitly. | — | `10` |
| `pre_provision` | boolean | no | `false` | `true`, `false` | Provision reusable traffic pods and Services before the suite. | — | `true` |
| `logs` | string | no | `ft-logs` | directory path | Relative to working directory; overridden by CLI output base. | — | `/tmp/tft-logs` |
| `runtime_class_name` | string | no | cluster default | valid lowercase DNS subdomain | RuntimeClass for eligible traffic pods; must already exist. | — | `kata` |
| `privileged_pod` | boolean | no | `false` | `true`, `false` | Traffic-pod privilege default; node override wins over env. | `TFT_PRIVILEGED_POD` | `true` |
| `capabilities_pod` | mapping | no | `{add: []}` | only `add`: list of non-empty strings | Linux capabilities; node mapping replaces this mapping. | — | `{add: [NET_ADMIN]}` |
| `udn_primary_network` | mapping | no | UDN/L3/overlay | table below | Primary-network configuration only; secondary topology comes from case. | UDN variables below | `{mode: cudn, topology: layer3}` |
| `connections` | list | yes | — | non-empty list | Traffic tools/endpoints and per-connection settings. | — | See example below. |

## `connections[]`

| Field | Type | Required | Default | Allowed values | Description | Environment | Example |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `name` | string | no | `Connection TEST/N` | non-empty string | Connection log label. | — | `iperf` |
| `type` | enum/string | no | `iperf-tcp` | `iperf-tcp`, `iperf-udp`, `http`, `netperf-tcp-stream`, `netperf-tcp-rr`, `ib-write-bw`, `ib-read-bw`, `ib-send-bw`, `simple` | Selects tool; enum names also accepted. | image variables | `http` |
| `test_cases` | selection | no | no additions | same syntax as test-level selection | Union with shared cases; empty string selects all. | — | `"5,6"` |
| `instances` | integer | no | 1 | positive integer | Number of instances; currently executed sequentially. | — | `2` |
| `reverse` | boolean | no | `true` | `true`, `false` | Additional reverse run only for iperf TCP. | — | `false` |
| `duration` | integer | no | test duration | non-negative seconds | Override this connection; zero becomes 3600. | — | `5` |
| `server` | list | one entry for a usable run | empty list | at most one endpoint | Configured server node and server options. | — | `[{name: worker-1}]` |
| `client` | list | one entry for a usable run | empty list | at most one endpoint | Configured client node and client options. | — | `[{name: worker-2}]` |
| `plugins` | list | no | empty list | plugin strings or mappings | Side measurements/checks; see plugins page. | — | `[measure_cpu]` |
| `secondary_network_nad` | string | no | `tft-secondary` when needed | `name` or `namespace/name` | Regular secondary network; existing NAD reused or OVN NAD generated. Node override wins. | `TFT_SECONDARY_NAD_*` | `tft-docs/secondary` |
| `resource_name` | string | no | unset/autodetected | extended resource name | VF resource from NAD annotation when detected; otherwise explicitly set. | — | `openshift.io/sriov_net` |
| `cpu_request` | string | no | unset | Kubernetes quantity | CPU request per traffic pod. | — | `100m` |
| `cpu_limit` | string | no | unset | Kubernetes quantity | CPU limit per traffic pod. | — | `1000m` |
| `mem_request` | string | no | unset | Kubernetes quantity | Memory request per traffic pod. | — | `64Mi` |
| `mem_limit` | string | no | unset | Kubernetes quantity | Memory limit per traffic pod. | — | `256Mi` |
| `egress_ip` | mapping | for case 68 | unset | table below | OVN EgressIP configuration. | — | `{ip: 192.0.2.100}` |

## `server[]` and `client[]`

| Field | Type | Required | Default | Allowed values | Description | Environment | Example |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `name` | string | yes | — | DNS node name | Server node anchors same-node placement; client node used for different-node/external cases. | — | `worker-1` |
| `sriov` | boolean | no | `false` | `true`, `false` | Use SR-IOV default attachment on eligible traffic endpoints. | — | `true` |
| `default_network` | string | no | `default/default` | NAD reference | Default attachment for SR-IOV traffic pods; legacy alias `default-network` is accepted. | — | `tft-docs/sriov` |
| `secondary_network_nad` | string | no | connection setting | NAD reference | Overrides regular secondary NAD for this endpoint. Case-generated UDN NADs remain case-specific. | — | `tft-docs/secondary-client` |
| `runtime_class_name` | string | no | test setting | valid lowercase DNS subdomain | Override runtime for eligible pods on this node. | — | `kata-coldplug` |
| `privileged_pod` | boolean | no | env, then test setting | `true`, `false` | Explicit node value takes highest precedence. | `TFT_PRIVILEGED_POD` | `false` |
| `capabilities_pod` | mapping | no | test setting | `{add: [CAPABILITY, ...]}` | Replaces test capability list, does not merge it. | — | `{add: [NET_ADMIN]}` |
| `args` | string or string list | no | none | iperf, simple, RDMA only | Additional tool arguments; strings use shell word splitting. | — | `["-b", "10G"]` |
| `persistent` (server only) | boolean | no | `false` | `true`, `false` | Keep the server process available within suite; final cleanup still removes pod. | — | `true` |
| `pod_port` (server only) | integer | no | 5201 | 1–65535 | Base server port for pod endpoints; use unique ports across connections. | — | `5202` |
| `host_port` (server only) | integer | no | 5301 | 1–65535 | Base server port for host-backed endpoints; use unique ports across connections. | — | `5302` |

## `plugins[]`

| Field | Type | Required | Default | Allowed values | Description | Environment | Example |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `name` | string | yes for mapping | — | `measure_cpu`, `measure_power`, `validate_offload`, `ovs_doca_validate_offload`, `ping_mgmt_port` | Plain string shorthand also accepted. | — | `measure_cpu` |
| `test_cases` | selection | no | every selected case | IDs/names/ranges/list | Filters plugin application; does not add traffic cases. | — | `[1, 2]` |

## `egress_ip`

| Field | Type | Required | Default | Allowed values | Description | Environment | Example |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `ip` | string | yes | — | IPv4/IPv6 address | Must be in an egress-node host CIDR and available for assignment. | — | `192.0.2.100` |
| `node` | string | no | configured client node | cluster node name | Node marked egress-assignable. | — | `worker-2` |

## `udn_primary_network`

| Field | Type | Required | Default | Allowed values | Description | Environment | Example |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `name` | string | no | `tft-primary` | non-empty; API-valid name | Generated primary UDN/CUDN name; choose a valid Kubernetes name. | `TFT_EXISTING_PRIMARY_CUDN` uses supplied resource | `blue` |
| `mode` | enum/string | no | `udn` | `udn`, `cudn` | Namespaced UDN vs cluster-scoped CUDN. | — | `cudn` |
| `topology` | enum/string | no | `layer3` | `layer3`, `layer2` | Primary topology; localnet is only available through secondary case IDs. | — | `layer2` |
| `transport` | enum/string | no | `overlay` | `overlay`, `no-overlay` | No-overlay requires primary CUDN Layer3. | no-overlay env variables | `no-overlay` |
| `uplink_name` | string | no | unset | pre-existing Uplink name | Primary CUDN reference; TFT does not create or delete Uplink. | — | `blue-uplink` |
| `route_advertisement` | mapping | no | unset | table below | Only with no-overlay; must be omitted in managed-routing mode. | `TFT_UDN_NO_OVERLAY_ROUTING_MANAGED` | See annotated example. |

## `route_advertisement`

| Field | Type | Required | Default | Allowed values | Description | Environment | Example |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `targetVRF` | string | no | omitted | API-supported `auto`, `default`, or configured VRF name | Copied directly; TFT only checks non-empty string. Omitted field preserves API behavior. | — | `auto` |
| `frr_configuration_selector` | mapping | yes | — | non-empty label-key/value map | Copied to matchLabels selector for existing FRRConfigurations; null values become empty strings. | — | `{network: blue}` |

## Precedence and scope

| Setting | Resolution, highest priority first |
| --- | --- |
| `duration` | Connection → test; omitted/zero test duration and explicit zero connection duration become 3600. No per-node duration. |
| `test_cases` | Union of test and connection selections, not replacement; plugin selection is a filter. |
| `runtime_class_name` | Endpoint → test → cluster default; host-network and tool pods use cluster default. No connection field. |
| `secondary_network_nad` | Endpoint → connection → `tft-secondary` for regular cases; UDN/CUDN secondary cases select their generated NAD. No test-level field. |
| `privileged_pod` | Endpoint → `TFT_PRIVILEGED_POD` → test (`false` by default). No connection field. |
| `capabilities_pod` | Endpoint mapping → test mapping; replaces rather than merges. |
| Kubeconfigs | CLI pair → environment pair → YAML pair → file detection. See [running tests](../getting-started/running-tests.md#kubeconfig-lookup). |
| Output location | CLI `-o` → test `logs` → `ft-logs`. |

There is no generic “environment overrides every YAML value” rule. Image, manifest,
target-access, external-server, and UDN environment variables have specific consumers;
see [environment variables](environment-variables.md).

## Annotated configuration

This schema example shows all supported fields, including optional features that require
separate cluster preparation. It is **not** a starter suite: replace site values and
remove features you do not use. For a runnable minimal example start with
[quickstart](../../examples/configs/quickstart.yaml).

```yaml
kubeconfig: /path/tenant.yaml                 # Tenant cluster access.
kubeconfig_infra: /path/infra.yaml            # Selecting infra access enables DPU mode.
dpu_node_host_label: provisioning.dpu.nvidia.com/host
tft:
  - name: configured-suite
    namespace: tft-docs
    test_cases: [1, 2]                        # Shared cases on every connection.
    duration: 10
    pre_provision: true
    logs: ft-logs
    runtime_class_name: kata                  # Must exist and schedule on selected nodes.
    privileged_pod: false
    capabilities_pod: {add: [NET_ADMIN]}
    udn_primary_network:                      # Used only by selected primary UDN cases.
      name: blue
      mode: cudn
      topology: layer3
      transport: no-overlay
      uplink_name: blue-uplink                # Pre-existing; TFT does not own it.
      route_advertisement:                   # Unmanaged no-overlay routing only.
        targetVRF: auto
        frr_configuration_selector: {network: blue}
    connections:
      - name: iperf
        type: iperf-tcp
        test_cases: [5, 6]                    # Added to the shared cases.
        instances: 1
        reverse: false
        duration: 5                          # Connection override.
        secondary_network_nad: tft-docs/secondary
        resource_name: openshift.io/sriov_net
        cpu_request: "100m"
        cpu_limit: "1000m"
        mem_request: "64Mi"
        mem_limit: "256Mi"
        egress_ip:                            # Required when selecting case 68.
          ip: 192.0.2.100
          node: worker-2
        server:
          - name: worker-1
            sriov: true
            default_network: tft-docs/sriov
            secondary_network_nad: tft-docs/secondary-server
            runtime_class_name: kata
            privileged_pod: true              # Takes priority over environment/test.
            capabilities_pod: {add: [NET_ADMIN]}
            args: []
            persistent: true
            pod_port: 5201
            host_port: 5301
        client:
          - name: worker-2
            sriov: true
            default_network: tft-docs/sriov
            secondary_network_nad: tft-docs/secondary-client
            runtime_class_name: kata
            privileged_pod: false
            capabilities_pod: {add: []}        # Clears the inherited capability list.
            args: ["--parallel", "2"]
        plugins:
          - name: measure_cpu
            test_cases: [1, 2]
```

## Next steps

- [Select cases](test-cases.md#selecting-test-cases)
- [Environment variables](environment-variables.md)
- [Run configurations](../getting-started/running-tests.md)
