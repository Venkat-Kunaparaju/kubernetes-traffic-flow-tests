<!--
SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
SPDX-License-Identifier: Apache-2.0
-->


# Kubernetes traffic flow tests

Traffic Flow Tests (TFT) checks Kubernetes connectivity, throughput, offload, and
network-policy behavior. You choose traffic paths and tools in YAML; TFT creates test
pods and, when needed, Services, policies, NADs, UDN/CUDN networks, and namespaces. It
records measurements and optional plugin checks, evaluates results, and normally cleans
up the resources it owns. Use a dedicated test namespace: TFT also changes security
labels on reused namespaces.

## Prerequisites

### CNI and feature support

Default-network pod/host, ClusterIP, NodePort, LoadBalancer, external, and ordinary
NetworkPolicy cases use standard Kubernetes resources. They can be used with another CNI
when that CNI implements the selected behavior; the presence of an API does not
establish data-path enforcement. This repository focuses on OVN-Kubernetes, and a
source-based description of a path is not a claim that every CNI or hardware combination
has been validated.

OVN-Kubernetes features are required for primary and secondary UDN/CUDN cases **37–47,
70–91, 100–101**, EgressIP **68**, the `ping_mgmt_port` plugin, and this repository's
OVN offload/DPU workflows (`validate_offload`, `ovs_doca_validate_offload`).
AdminNetworkPolicy **32–34** requires its CRD and CNI enforcement; it targets the OVN-K
workflow here, but the standard API can also be implemented by other CNIs.

`measure_cpu` and `measure_power` do not depend on OVN APIs in single-cluster mode.
Secondary-network/MultiNetworkPolicy cases **27–31** need Multus, usable NADs, and MNP
support for policy checks. Their automatically generated NAD uses OVN-Kubernetes; use
pre-existing endpoint NADs for another secondary CNI.

### Cluster, access, and tools

- Use at least two Ready worker nodes for different-node cases. Same-node cases
  can run on a single worker; examples include **1, 3, 5, 7, 9, 11, 13, 15**.
  The [catalog](docs/configuring/test-cases.md) lists placement for every case.
- The runner needs `kubectl`, a recommended Python **3.11** environment, and
  a kubeconfig supplied through TFT's CLI, environment, or YAML. TFT does not
  automatically use the usual `~/.kube/config` / `KUBECONFIG` defaults.
- Grant read access to nodes and selected RuntimeClasses, create/update/delete
  access to namespaces, pods (including exec), Services, and the feature
  resources in the suite. Network cases can need NADs, policies, UDN/CUDN,
  RouteAdvertisements, Uplink/FRR reads, EgressIP, and node labelling. Plugin
  pods require privileged/host-network/host-filesystem access. In practice,
  running all features usually requires cluster-admin-equivalent access;
  a narrower role must match the selected suite's actual operations.
- Cluster nodes need access to the test container image, or set `TFT_TEST_IMAGE`
  to a mirrored image and arrange cluster image-pull credentials. See
  [installation](docs/getting-started/install.md) for image and pull-policy rules.

| Feature | Prepare before running TFT | Guide |
| --- | --- | --- |
| Ordinary Services | Working ClusterIP/NodePort routing and DNS for name variants | [Services](docs/configuring/scenarios/services.md) |
| LoadBalancer | Provider/address allocation, such as MetalLB, and reachable external IPs | [Services](docs/configuring/scenarios/services.md) |
| Secondary networks | Multus and an appropriate NAD/CNI; generated NAD path is OVN-specific | [Secondary networks](docs/configuring/scenarios/secondary-networks-sriov.md) |
| SR-IOV | SR-IOV operator or equivalent VF/device-plugin setup, NAD and extended resource | [SR-IOV](docs/configuring/scenarios/secondary-networks-sriov.md) |
| NP / MNP | Enforcing CNI; MNP also needs its CRD and network attachment | [Network policy](docs/configuring/scenarios/network-policy.md) |
| ANP | AdminNetworkPolicy CRD and CNI enforcement | [AdminNetworkPolicy](docs/configuring/scenarios/admin-network-policy.md) |
| UDN / CUDN | OVN-Kubernetes feature/CRDs; localnet needs physical-network mapping | [UDN/CUDN](docs/configuring/scenarios/udn-cudn.md) |
| No-overlay route advertisement | Supported OVN transport, underlay routes, appropriate FRR setup; existing Uplink when referenced | [UDN/CUDN routing](docs/configuring/scenarios/udn-cudn.md#no-overlay-primary-cudn) |
| EgressIP | OVN EgressIP support and an assignable site address | [EgressIP](docs/configuring/scenarios/egress-ip.md) |
| DPU offload | Site DPU/operator deployment, paired infra kubeconfig/labels, VF and representor access | [DPU mode](docs/configuring/scenarios/dpu-mode.md) |
| RDMA | RDMA NICs/devices exposed to pods, usable fabric/transport, RDMA image | [RDMA](docs/configuring/scenarios/rdma.md) |
| RuntimeClass | Installed runtime such as Kata, RuntimeClass object, compatible selected nodes | [RuntimeClass](docs/configuring/scenarios/runtime-class.md) |
| External traffic | Reachable Linux runner with Podman, or a pre-existing matching tool server/HTTP URL | [External traffic](docs/configuring/scenarios/external-traffic.md) |
| Power sampling | Working node IPMI/DCMI sensors and privileged plugin pods | [Plugins](docs/configuring/plugins.md) |

On OpenShift, set `TFT_HOST_NETWORK_NAMESPACE=openshift-host-network` for case 69.
Security admission must permit the traffic and plugin pod types you select; see the
scenario prerequisites and [environment
reference](docs/configuring/environment-variables.md).

## Quick start

After [installation](docs/getting-started/install.md), run from the repo root:

```bash
export TFT_KUBECONFIG="$HOME/.kube/config"
kubectl --kubeconfig "$TFT_KUBECONFIG" get nodes -o wide
cp examples/configs/quickstart.yaml quickstart.yaml
${EDITOR:-vi} quickstart.yaml
./tft.py --check quickstart.yaml
```
 Replace the example's two worker names in the editor. This runs cases 1 and 2 for ten
seconds each, without plugins or reverse subtests. Read the result path in the console
and run `./print_results.py --no-color RESULT.json` on that file. For a single worker
choose case 1 only. The [full walkthrough](docs/getting-started/quickstart.md) explains
success and cleanup.

## Documentation directory

### Getting started

| Page | Use it for |
| --- | --- |
| [Install TFT](docs/getting-started/install.md) | You need to prepare the machine that runs TFT. |
| [Run your first test](docs/getting-started/quickstart.md) | You want a short pod-to-pod connectivity and throughput check. |
| [Run and control tests](docs/getting-started/running-tests.md) | You need command-line options, kubeconfig rules, or cleanup details. |

### Configuring

| Page | Use it for |
| --- | --- |
| [Configuration reference](docs/configuring/config-reference.md) | You need exact fields, defaults, or override rules. |
| [Environment variables](docs/configuring/environment-variables.md) | You need image, cluster, manifest, network, or logging overrides. |
| [Add plugins](docs/configuring/plugins.md) | You need CPU, power, offload, or management-port evidence alongside traffic. |
| [Choose test cases](docs/configuring/test-cases.md) | You need traffic paths, prerequisites, or case-selection syntax. |
| [Choose a traffic tool](docs/configuring/test-types.md) | You need to decide what to measure and which arguments to use. |

### Scenarios

| Page | Use it for |
| --- | --- |
| [AdminNetworkPolicy](docs/configuring/scenarios/admin-network-policy.md) | You need cluster-level Allow, Deny, and Pass checks. |
| [Basic pod networking](docs/configuring/scenarios/basic-pod-networking.md) | You want to compare pod and host connectivity. |
| [DPU mode and offload](docs/configuring/scenarios/dpu-mode.md) | You need paired tenant/infra clusters and representor validation on the DPU. |
| [EgressIP](docs/configuring/scenarios/egress-ip.md) | You need to verify the source address observed by an external server. |
| [External traffic](docs/configuring/scenarios/external-traffic.md) | You need pod or host egress to an endpoint outside Kubernetes. |
| [NetworkPolicy and MultiNetworkPolicy](docs/configuring/scenarios/network-policy.md) | You need positive and negative policy checks. |
| [RDMA traffic](docs/configuring/scenarios/rdma.md) | You need perftest write, read, or send bandwidth measurements. |
| [Traffic-pod resource limits](docs/configuring/scenarios/resource-limits.md) | You want throughput under explicit CPU or memory budgets. |
| [RuntimeClass](docs/configuring/scenarios/runtime-class.md) | You want traffic pods to run with Kata or another installed runtime. |
| [Secondary networks and SR-IOV](docs/configuring/scenarios/secondary-networks-sriov.md) | You need a second interface or a VF-backed default attachment. |
| [Service traffic](docs/configuring/scenarios/services.md) | You want ClusterIP, NodePort, or LoadBalancer coverage. |
| [UDN and CUDN traffic](docs/configuring/scenarios/udn-cudn.md) | You need primary-network, secondary-network, or isolation coverage. |

### Results

| Page | Use it for |
| --- | --- |
| [Generate a baseline](docs/results/baselines.md) | You want thresholds based on successful known-good measurements. |
| [Evaluate pass and fail](docs/results/evaluation.md) | You need thresholds and reliable CI exit status. |
| [Output files and current schema](docs/results/output-files.md) | You need artifact paths or fields for a result-processing script. |
| [Read and compare results](docs/results/reading-results.md) | You have result JSON and need to interpret the measurements. |

### Examples

| Directory | Use it for |
| --- | --- |
| [examples/](examples/README.md) | Commented scenario configs and sample evaluation YAML. |

## I want to…

| Task | Start here |
| --- | --- |
| Run my first test | [Quickstart](docs/getting-started/quickstart.md) |
| Choose traffic paths and case IDs | [Test catalog](docs/configuring/test-cases.md) |
| Change a configuration field | [Configuration reference](docs/configuring/config-reference.md) |
| Test UDN/CUDN or no-overlay | [UDN/CUDN](docs/configuring/scenarios/udn-cudn.md) |
| Validate DPU offload | [DPU mode](docs/configuring/scenarios/dpu-mode.md) |
| Diagnose policy enforcement | [NetworkPolicy/MNP](docs/configuring/scenarios/network-policy.md) |
| Read a saved result | [Reading results](docs/results/reading-results.md) |
| Set CI pass/fail thresholds | [Evaluation](docs/results/evaluation.md) |
| Learn thresholds from known-good runs | [Baselines](docs/results/baselines.md) |

## Community and contributing

See [CONTRIBUTING](CONTRIBUTING.md) for developer setup and contribution rules,
[GOVERNANCE](GOVERNANCE.md), [MAINTAINERS](MAINTAINERS.md), and [MEETINGS](MEETINGS.md)
for project participation. The source is licensed under [Apache 2.0](LICENSE).
