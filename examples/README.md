<!--
SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
SPDX-License-Identifier: Apache-2.0
-->

# Example configurations

Use this page when you want a configuration to copy and adapt.

Run these configurations from the repository root after replacing the node, NAD, device,
address, and kubeconfig placeholders noted in each header. They have been checked with
TFT's configuration parser; specialized scenarios also require site infrastructure and
are not runtime validation evidence.

```bash
cp examples/configs/quickstart.yaml quickstart.yaml
./tft.py --check --kubeconfig /path/tenant.yaml quickstart.yaml
```

| File | Use / additional prerequisites |
| --- | --- |
| [quickstart](configs/quickstart.yaml) | Two pod connectivity cases, ten seconds, no plugins. |
| [smoke](configs/smoke.yaml) | Pod IP and ClusterIP cases 1,2,5,6. |
| [full-default-network](configs/full-default-network.yaml) | Pod/host, ClusterIP, both NodePort families; no LB or external prerequisite. |
| [basic-pod-networking](configs/basic-pod-networking.yaml) | Direct pod/host comparisons. |
| [services](configs/services.yaml) | ClusterIP plus both NodePort targets. |
| [external-traffic](configs/external-traffic.yaml) | Reachable runner Podman or pre-existing tool server. |
| [secondary-networks-sriov](configs/secondary-networks-sriov.yaml) | Multus and pre-existing endpoint NADs. |
| [network-policy](configs/network-policy.yaml) | NetworkPolicy enforcement; HTTP deny/allow. |
| [admin-network-policy](configs/admin-network-policy.yaml) | ANP CRD and enforcement; HTTP Allow/Deny/Pass. |
| [udn-cudn](configs/udn-cudn.yaml) | OVN-K primary UDN connectivity. |
| [egress-ip](configs/egress-ip.yaml) | OVN EgressIP, site address and runner-side Podman server. |
| [dpu-mode](configs/dpu-mode.yaml) | Paired kubeconfigs/labels, VF-backed NAD and offload deployment. |
| [runtime-class](configs/runtime-class.yaml) | Kata RuntimeClass and matching nodes. |
| [resource-limits](configs/resource-limits.yaml) | Explicit CPU/memory budgets on ordinary pods. |
| [rdma](configs/rdma.yaml) | RDMA devices exposed to both pods and usable transport. |
| [diverse-subset](configs/diverse-subset.yaml) | Copy of the CI suite; includes ANP, NP and external cases, so it needs more than basic pod networking. |
| [smoke evaluation](eval/smoke.yaml) | Illustrative thresholds; tune to your hardware. |
| [directional evaluation](eval/directional.yaml) | Separate TX/RX thresholds. |
| [fixture baseline](eval/fixture-baseline.yaml) | Small excerpt from the baseline-generator expected-output fixture. |

The root [config.yaml](../config.yaml) remains the original example, and
[.github/tft-configs](../.github/tft-configs) contains configurations used by CI. Those
CI files carry simulation node names and feature-specific assumptions; start with the
simpler files above for an ordinary cluster.

## Next steps

- [Scenario guides](../docs/configuring/scenarios/basic-pod-networking.md)
- [Evaluation](../docs/results/evaluation.md)
- [Generate a baseline](../docs/results/baselines.md)
