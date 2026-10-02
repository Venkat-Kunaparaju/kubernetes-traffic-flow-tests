<!--
SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
SPDX-License-Identifier: Apache-2.0
-->

# Environment variables

Use this page when you need image, cluster, manifest, network, or logging overrides.

Set variables before starting TFT; many getters cache values for the process. This table
covers the `TFT_*` variables consumed by this upstream runner. A dash in YAML
correspondence would not imply a generic override mechanism: most variables configure
behavior with no YAML field.

## Images

| Variable | Default | Configuration relationship | Example value |
| --- | --- | --- | --- |
| `TFT_TEST_IMAGE` | `ghcr.io/ovn-kubernetes/kubernetes-traffic-flow-tests:latest` | No YAML field; base pod/tool image | `registry.example.com/tft:tested` |
| `TFT_RDMA_TEST_IMAGE` | Base name + `-rdma` before tag | No YAML field; `ib-*` image | `registry.example.com/tft-rdma:tested` |
| `TFT_IMAGE_PULL_POLICY` | `IfNotPresent`; `Always` when base image set | No YAML field; imagePullPolicy | `IfNotPresent` |

## Pod behavior and access

| Variable | Default | Configuration relationship | Example value |
| --- | --- | --- | --- |
| `TFT_PRIVILEGED_POD` | unset | Test `privileged_pod`; endpoint value wins | `true` |
| `TFT_POD_BRINGUP_TIMEOUT` | `2m` | No YAML field; readiness timeout | `5m` |
| `TFT_KUBECONFIG` | unset | Top-level `kubeconfig`, below CLI | `/path/tenant.yaml` |
| `TFT_KUBECONFIG_INFRA` | unset | Top-level `kubeconfig_infra`, below CLI; pair rule applies | `/path/infra.yaml` |
| `TFT_HOST_NETWORK_NAMESPACE` | `ovn-host-network` | No YAML field; case 69 namespaceSelector target | `openshift-host-network` |

## Manifests

| Variable | Default | Configuration relationship | Example value |
| --- | --- | --- | --- |
| `TFT_MANIFESTS_OVERRIDES` | repo `manifests/overrides` | No YAML field; preferred template directory; empty disables overrides | `/tmp/tft-templates` |
| `TFT_MANIFESTS_YAMLS` | repo `manifests/yamls` | No YAML field; rendered-manifest directory must exist | `/tmp/tft-rendered` |

## Target access

| Variable | Default | Configuration relationship | Example value |
| --- | --- | --- | --- |
| `TFT_DEFAULT_TARGET_ACCESS_MODE` | `IP` for ClusterIP/LoadBalancer | No YAML field; only `IP` or `SERVICE_NAME`; NodePort unaffected | `SERVICE_NAME` |
| `TFT_ENABLE_TARGET_ACCESS_SUBTESTS` | `false` | No YAML field; adds Service IP/DNS and respective node-IP variants | `true` |

## UDN and CUDN

| Variable | Default | Configuration relationship | Example value |
| --- | --- | --- | --- |
| `TFT_EXISTING_PRIMARY_CUDN` | unset | Uses a user-owned primary CUDN instead of generating primary network | `blue` |
| `TFT_UDN_PRIMARY_CIDR` | `15.1.0.0/16`, host subnet 24 | No YAML field; primary subnets; comma-separated CIDRs with optional host prefix | `15.1.0.0/17/24,15.1.128.0/17/24` |
| `TFT_CUDN_SECONDARY_LAYER3_CIDR` | `15.2.0.0/16` | No YAML field; generated secondary CUDN Layer3 subnet | `172.20.0.0/16` |
| `TFT_UDN_SECONDARY_LAYER3_CIDR` | `15.3.0.0/16` | No YAML field; generated secondary UDN Layer3 subnet | `172.21.0.0/16` |
| `TFT_CUDN_SECONDARY_LAYER2_CIDR` | `15.4.0.0/16` | No YAML field; generated secondary CUDN Layer2 subnet | `172.22.0.0/16` |
| `TFT_UDN_SECONDARY_LAYER2_CIDR` | `15.5.0.0/16` | No YAML field; generated secondary UDN Layer2 subnet | `172.23.0.0/16` |
| `TFT_CUDN_SECONDARY_LOCALNET_CIDR` | `15.6.0.0/24` | No YAML field; generated localnet subnet | `172.24.0.0/24` |
| `TFT_CUDN_SECONDARY_LAYER2_IPAM_MODE` | omitted | No YAML field; passed through as ipam.mode | `Enabled` |
| `TFT_UDN_SECONDARY_LAYER2_IPAM_MODE` | omitted | No YAML field; passed through as ipam.mode | `Enabled` |
| `TFT_CUDN_SECONDARY_LOCALNET_IPAM_MODE` | omitted | No YAML field; passed through as ipam.mode | `Enabled` |
| `TFT_CUDN_LOCALNET_PHYSICAL_NETWORK` | `physnet` | No YAML field; localnet physical network name | `physnet-blue` |
| `TFT_UDN_NO_OVERLAY_OUTBOUND_SNAT_ENABLED` | `true` | No YAML field; outbound SNAT for generated no-overlay CUDN | `false` |
| `TFT_UDN_NO_OVERLAY_ROUTING_MANAGED` | `false` | No YAML field; generated no-overlay routing mode | `true` |

## Regular secondary NAD

| Variable | Default | Configuration relationship | Example value |
| --- | --- | --- | --- |
| `TFT_SECONDARY_NAD_SUBNETS` | `10.193.0.0/16/26` | No YAML field; used only when generating an OVN NAD | `10.194.0.0/16/26` |
| `TFT_SECONDARY_NAD_MTU` | `1500` | No YAML field; generated OVN NAD MTU | `1400` |
| `TFT_SECONDARY_NAD_TOPOLOGY` | `layer3` | No YAML field; generated OVN NAD topology | `layer2` |

## External endpoints

| Variable | Default | Configuration relationship | Example value |
| --- | --- | --- | --- |
| `TFT_EXTERNAL_SERVER` | unset; runner Podman server | No YAML field; pre-existing server address and optional port | `192.0.2.10:5201` |
| `TFT_EXTERNAL_URL` | unset | No YAML field; HTTP URL in EXTERNAL_IP mode | `https://example.com/health` |
| `TFT_EXTERNAL_SERVER_STRING` | `The document has moved` when URL used | No YAML field; expected substring in HTTP body | `healthy` |

## Logging

| Variable | Default | Configuration relationship | Example value |
| --- | --- | --- | --- |
| `TFT_LOG_PREAMBLE` | `true` | No YAML field; timestamp/thread log preamble | `false` |

## Details that affect a run

- Kubeconfigs must come as a tenant/infra pair from the same source; see
  [lookup rules](../getting-started/running-tests.md#kubeconfig-lookup).
- Per-endpoint `privileged_pod` wins over `TFT_PRIVILEGED_POD`, which wins over
  the test-level setting. Plugin tool pods are separately privileged.
- `TFT_RDMA_TEST_IMAGE` defaults to `<image>-rdma:<tag>`, not the base image's
  `:rdma` tag. Set it explicitly for other tag or digest conventions.
- Create `TFT_MANIFESTS_YAMLS` before running. Relative override/output paths
  resolve from the current working directory; defaults resolve from the repo.
- Layer2/localnet IPAM modes are not validated by TFT. Subnets are rendered
  only when the mode is unset or `Enabled`; other modes need usable addresses
  supplied by your deployment. “Mode Disabled” does not make TFT allocate IPs.
- `TFT_EXTERNAL_SERVER` accepts `host[:port]`, including `[fd00::1]:5201`.
  Without a port it uses the configured server `pod_port`. External servers
  must speak the connection's tool protocol. `TFT_EXTERNAL_URL` applies to HTTP
  in EXTERNAL_IP mode, including primary UDN external traffic.
- On OpenShift set `TFT_HOST_NETWORK_NAMESPACE=openshift-host-network` for case
  69. The target namespace must exist and match the policy selector.

The shared logging dependency also honors `KTOOLBOX_LOGLEVEL` and
`KTOOLBOX_ALL_LOGGERS`; TFT's `--verbosity` / `--verbose` options configure its logging.
`NO_COLOR` disables color in `print_results.py`.

## Next steps

- [Configuration reference](config-reference.md#precedence-and-scope)
- [Install and images](../getting-started/install.md)
- [UDN/CUDN scenarios](scenarios/udn-cudn.md)
