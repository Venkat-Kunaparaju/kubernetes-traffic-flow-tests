<!--
SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
SPDX-License-Identifier: Apache-2.0
-->

# Choose test cases

Use this page when you need traffic paths, prerequisites, or case-selection syntax.

The catalog below contains every member of `TestCaseType` in
[`tftbase.py`](../../tftbase.py), including its traffic-path metadata. IDs 48–59 are not
assigned. Select the IDs relevant to your cluster rather than using `"*"` for a first
run.

**Placement:** same-node cases put both endpoints on the configured server node;
different-node cases use the server and client node names. Policy cases without a
placement suffix are different-node cases. External cases run the client on the
configured client node and use a server outside Kubernetes.

**Network:** “default” is the cluster default network; `sriov: true` can replace an
eligible endpoint's default attachment. “Primary” uses the test's `udn_primary_network`.
“CDN” in cases 80–81 means cluster default network.

**Test types:** `general` means iperf TCP/UDP, HTTP, netperf TCP stream/RR, and simple
have handlers, with no case/type allowlist in the parser. It is not a promise that every
tool works on every path. `policy` recommends iperf TCP or HTTP: these handlers
explicitly recognize expected blocks; other handlers have limitations. `RDMA*` is
conditional on RDMA-capable endpoints and transport; Service and policy support for
ordinary TCP/UDP does not establish RDMA support. See [test types](test-types.md) before
choosing RDMA or netperf RR.

**OVN-K requirement:** “yes” denotes OVN APIs or a generated OVN NAD. “ANP support”
denotes a standard API whose CRD and CNI enforcement must be present; these cases are
intended for the OVN-K workflow but are not intrinsically exclusive to OVN-K. Cases
27–31 can use another CNI's pre-existing NAD and MNP implementation, but TFT's
automatically generated NAD is OVN-specific.

**Resources:** all cases create traffic pods (or an external server) and use test
namespaces; table cells list additional resources. Runtime-specific controllers can
create backing resources, such as a NAD for a UDN.

Expected-block cases pass when traffic is blocked. A missing throughput value is normal
there, but a failed connection can also reflect broken infrastructure. Run a matching
allow case to establish that the path works. Case 31 installs a policy denying the
**second** interface and expects primary-interface traffic to **pass** despite `DENY` in
its name. Cases 80–81 expect isolation even without `DENY` in their names.

## Pod and host

Direct pod IP or host-network pod IP traffic. Host endpoints share the node network
namespace.

| ID / Name | Client → path → Server | Placement | Network | Expected | Requires OVN-K? | Other requirements | Test types | Additional resources |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 / `POD_TO_POD_SAME_NODE` | Pod → direct IP → Pod | same | default | passes | no | Ready node(s) | general + RDMA* | — |
| 2 / `POD_TO_POD_DIFF_NODE` | Pod → direct IP → Pod | different | default | passes | no | Ready node(s) | general + RDMA* | — |
| 3 / `POD_TO_HOST_SAME_NODE` | Pod → direct IP → Host-network pod | same | default | passes | no | Ready node(s) | general + RDMA* | — |
| 4 / `POD_TO_HOST_DIFF_NODE` | Pod → direct IP → Host-network pod | different | default | passes | no | Ready node(s) | general + RDMA* | — |
| 13 / `HOST_TO_HOST_SAME_NODE` | Host-network pod → direct IP → Host-network pod | same | default | passes | no | Ready node(s) | general + RDMA* | — |
| 14 / `HOST_TO_HOST_DIFF_NODE` | Host-network pod → direct IP → Host-network pod | different | default | passes | no | Ready node(s) | general + RDMA* | — |
| 15 / `HOST_TO_POD_SAME_NODE` | Host-network pod → direct IP → Pod | same | default | passes | no | Ready node(s) | general + RDMA* | — |
| 16 / `HOST_TO_POD_DIFF_NODE` | Host-network pod → direct IP → Pod | different | default | passes | no | Ready node(s) | general + RDMA* | — |

## ClusterIP

Traffic goes through a ClusterIP Service. The normal target is the Service IP (`IP`).

`TFT_ENABLE_TARGET_ACCESS_SUBTESTS=true` adds `IP` and `SERVICE_NAME` targets. NodePort
also retains its named node-IP target. `TFT_DEFAULT_TARGET_ACCESS_MODE` changes only the
normal ClusterIP/LoadBalancer target; NodePort selection comes from the case ID.

| ID / Name | Client → path → Server | Placement | Network | Expected | Requires OVN-K? | Other requirements | Test types | Additional resources |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 5 / `POD_TO_CLUSTER_IP_TO_POD_SAME_NODE` | Pod → ClusterIP Service → Pod | same | default | passes | no | Ready node(s) | general + RDMA* | Service |
| 6 / `POD_TO_CLUSTER_IP_TO_POD_DIFF_NODE` | Pod → ClusterIP Service → Pod | different | default | passes | no | Ready node(s) | general + RDMA* | Service |
| 7 / `POD_TO_CLUSTER_IP_TO_HOST_SAME_NODE` | Pod → ClusterIP Service → Host-network pod | same | default | passes | no | Ready node(s) | general + RDMA* | Service |
| 8 / `POD_TO_CLUSTER_IP_TO_HOST_DIFF_NODE` | Pod → ClusterIP Service → Host-network pod | different | default | passes | no | Ready node(s) | general + RDMA* | Service |
| 17 / `HOST_TO_CLUSTER_IP_TO_POD_SAME_NODE` | Host-network pod → ClusterIP Service → Pod | same | default | passes | no | Ready node(s) | general + RDMA* | Service |
| 18 / `HOST_TO_CLUSTER_IP_TO_POD_DIFF_NODE` | Host-network pod → ClusterIP Service → Pod | different | default | passes | no | Ready node(s) | general + RDMA* | Service |
| 19 / `HOST_TO_CLUSTER_IP_TO_HOST_SAME_NODE` | Host-network pod → ClusterIP Service → Host-network pod | same | default | passes | no | Ready node(s) | general + RDMA* | Service |
| 20 / `HOST_TO_CLUSTER_IP_TO_HOST_DIFF_NODE` | Host-network pod → ClusterIP Service → Host-network pod | different | default | passes | no | Ready node(s) | general + RDMA* | Service |

## NodePort through the client node

`NODE_PORT_TO` uses the configured client node InternalIP and allocated NodePort
(`CLIENT_NODE_IP`).

`TFT_ENABLE_TARGET_ACCESS_SUBTESTS=true` adds `IP` and `SERVICE_NAME` targets. NodePort
also retains its named node-IP target. `TFT_DEFAULT_TARGET_ACCESS_MODE` changes only the
normal ClusterIP/LoadBalancer target; NodePort selection comes from the case ID.

| ID / Name | Client → path → Server | Placement | Network | Expected | Requires OVN-K? | Other requirements | Test types | Additional resources |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 9 / `POD_TO_NODE_PORT_TO_POD_SAME_NODE` | Pod → client-node NodePort → Pod | same | default | passes | no | Ready node(s) | general + RDMA* | Service |
| 10 / `POD_TO_NODE_PORT_TO_POD_DIFF_NODE` | Pod → client-node NodePort → Pod | different | default | passes | no | Ready node(s) | general + RDMA* | Service |
| 11 / `POD_TO_NODE_PORT_TO_HOST_SAME_NODE` | Pod → client-node NodePort → Host-network pod | same | default | passes | no | Ready node(s) | general + RDMA* | Service |
| 12 / `POD_TO_NODE_PORT_TO_HOST_DIFF_NODE` | Pod → client-node NodePort → Host-network pod | different | default | passes | no | Ready node(s) | general + RDMA* | Service |
| 21 / `HOST_TO_NODE_PORT_TO_POD_SAME_NODE` | Host-network pod → client-node NodePort → Pod | same | default | passes | no | Ready node(s) | general + RDMA* | Service |
| 22 / `HOST_TO_NODE_PORT_TO_POD_DIFF_NODE` | Host-network pod → client-node NodePort → Pod | different | default | passes | no | Ready node(s) | general + RDMA* | Service |
| 23 / `HOST_TO_NODE_PORT_TO_HOST_SAME_NODE` | Host-network pod → client-node NodePort → Host-network pod | same | default | passes | no | Ready node(s) | general + RDMA* | Service |
| 24 / `HOST_TO_NODE_PORT_TO_HOST_DIFF_NODE` | Host-network pod → client-node NodePort → Host-network pod | different | default | passes | no | Ready node(s) | general + RDMA* | Service |

## NodePort through the server node

`NODE_PORT_SERVER_TO` uses the server node InternalIP and allocated NodePort
(`SERVER_NODE_IP`).

`TFT_ENABLE_TARGET_ACCESS_SUBTESTS=true` adds `IP` and `SERVICE_NAME` targets. NodePort
also retains its named node-IP target. `TFT_DEFAULT_TARGET_ACCESS_MODE` changes only the
normal ClusterIP/LoadBalancer target; NodePort selection comes from the case ID.

| ID / Name | Client → path → Server | Placement | Network | Expected | Requires OVN-K? | Other requirements | Test types | Additional resources |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 92 / `POD_TO_NODE_PORT_SERVER_TO_POD_SAME_NODE` | Pod → server-node NodePort → Pod | same | default | passes | no | Ready node(s) | general + RDMA* | Service |
| 93 / `POD_TO_NODE_PORT_SERVER_TO_POD_DIFF_NODE` | Pod → server-node NodePort → Pod | different | default | passes | no | Ready node(s) | general + RDMA* | Service |
| 94 / `POD_TO_NODE_PORT_SERVER_TO_HOST_SAME_NODE` | Pod → server-node NodePort → Host-network pod | same | default | passes | no | Ready node(s) | general + RDMA* | Service |
| 95 / `POD_TO_NODE_PORT_SERVER_TO_HOST_DIFF_NODE` | Pod → server-node NodePort → Host-network pod | different | default | passes | no | Ready node(s) | general + RDMA* | Service |
| 96 / `HOST_TO_NODE_PORT_SERVER_TO_POD_SAME_NODE` | Host-network pod → server-node NodePort → Pod | same | default | passes | no | Ready node(s) | general + RDMA* | Service |
| 97 / `HOST_TO_NODE_PORT_SERVER_TO_POD_DIFF_NODE` | Host-network pod → server-node NodePort → Pod | different | default | passes | no | Ready node(s) | general + RDMA* | Service |
| 98 / `HOST_TO_NODE_PORT_SERVER_TO_HOST_SAME_NODE` | Host-network pod → server-node NodePort → Host-network pod | same | default | passes | no | Ready node(s) | general + RDMA* | Service |
| 99 / `HOST_TO_NODE_PORT_SERVER_TO_HOST_DIFF_NODE` | Host-network pod → server-node NodePort → Host-network pod | different | default | passes | no | Ready node(s) | general + RDMA* | Service |

## LoadBalancer

The cluster must allocate a reachable LoadBalancer IP. The normal target is that IP
(`IP`). MetalLB is one possible provider.

`TFT_ENABLE_TARGET_ACCESS_SUBTESTS=true` adds `IP` and `SERVICE_NAME` targets. NodePort
also retains its named node-IP target. `TFT_DEFAULT_TARGET_ACCESS_MODE` changes only the
normal ClusterIP/LoadBalancer target; NodePort selection comes from the case ID.

| ID / Name | Client → path → Server | Placement | Network | Expected | Requires OVN-K? | Other requirements | Test types | Additional resources |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 60 / `POD_TO_LOAD_BALANCER_TO_POD_SAME_NODE` | Pod → LoadBalancer Service → Pod | same | default | passes | no | LB provider + reachable IP | general + RDMA* | Service |
| 61 / `POD_TO_LOAD_BALANCER_TO_POD_DIFF_NODE` | Pod → LoadBalancer Service → Pod | different | default | passes | no | LB provider + reachable IP | general + RDMA* | Service |
| 62 / `POD_TO_LOAD_BALANCER_TO_HOST_SAME_NODE` | Pod → LoadBalancer Service → Host-network pod | same | default | passes | no | LB provider + reachable IP | general + RDMA* | Service |
| 63 / `POD_TO_LOAD_BALANCER_TO_HOST_DIFF_NODE` | Pod → LoadBalancer Service → Host-network pod | different | default | passes | no | LB provider + reachable IP | general + RDMA* | Service |
| 64 / `HOST_TO_LOAD_BALANCER_TO_POD_SAME_NODE` | Host-network pod → LoadBalancer Service → Pod | same | default | passes | no | LB provider + reachable IP | general + RDMA* | Service |
| 65 / `HOST_TO_LOAD_BALANCER_TO_POD_DIFF_NODE` | Host-network pod → LoadBalancer Service → Pod | different | default | passes | no | LB provider + reachable IP | general + RDMA* | Service |
| 66 / `HOST_TO_LOAD_BALANCER_TO_HOST_SAME_NODE` | Host-network pod → LoadBalancer Service → Host-network pod | same | default | passes | no | LB provider + reachable IP | general + RDMA* | Service |
| 67 / `HOST_TO_LOAD_BALANCER_TO_HOST_DIFF_NODE` | Host-network pod → LoadBalancer Service → Host-network pod | different | default | passes | no | LB provider + reachable IP | general + RDMA* | Service |

## External traffic

A runner-side Podman server or an explicitly configured external server must be
reachable from the client. Case 68 validates an OVN EgressIP.

| ID / Name | Client → path → Server | Placement | Network | Expected | Requires OVN-K? | Other requirements | Test types | Additional resources |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 25 / `POD_TO_EXTERNAL` | Pod → external endpoint → External server | external | default | passes | no | external tool server or Podman; HTTP URL optional | general + RDMA* | — |
| 26 / `HOST_TO_EXTERNAL` | Host-network pod → external endpoint → External server | external | default | passes | no | external tool server or Podman; HTTP URL optional | general + RDMA* | — |
| 68 / `POD_TO_EXTERNAL_EGRESS` | Pod → external endpoint → External server | external | default | passes | yes | runner-side Podman iperf3 server; EgressIP + address in node host CIDRs | iperf TCP recommended | EgressIP + node label |

## Secondary network and MultiNetworkPolicy

The second interface uses a NAD. MNP cases require MultiNetworkPolicy CRD and
enforcement; case 31 checks that a deny on the second interface leaves primary traffic
working.

| ID / Name | Client → path → Server | Placement | Network | Expected | Requires OVN-K? | Other requirements | Test types | Additional resources |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 27 / `POD_TO_POD_2ND_INTERFACE_SAME_NODE` | Pod → second-interface IP → Pod | same | secondary | passes | generated NAD only | Multus + NAD | general + RDMA* | NAD if missing |
| 28 / `POD_TO_POD_2ND_INTERFACE_DIFF_NODE` | Pod → second-interface IP → Pod | different | secondary | passes | generated NAD only | Multus + NAD | general + RDMA* | NAD if missing |
| 29 / `POD_TO_POD_2ND_INTERFACE_MNP_ALLOW_2ND` | Pod → second-interface IP + MNP allow → Pod | different | secondary | passes | generated NAD only | Multus + NAD; MNP CRD + enforcement | policy | NAD if missing; MNP |
| 30 / `POD_TO_POD_2ND_INTERFACE_MNP_DENY_2ND` | Pod → second-interface IP + MNP deny → Pod | different | secondary | blocked | generated NAD only | Multus + NAD; MNP CRD + enforcement | policy | NAD if missing; MNP |
| 31 / `POD_TO_POD_PRIMARY_INTERFACE_MNP_DENY_2ND` | Pod → primary IP + second-interface MNP deny → Pod | different | primary | passes | generated NAD only | Multus + NAD; MNP CRD + enforcement | policy | NAD if missing; MNP |

## NetworkPolicy and AdminNetworkPolicy

Policies target test traffic. ANP Pass delegates to NetworkPolicy; case 34 therefore
expects a block. Case 69 tests namespaceSelector handling for a host-network client.

| ID / Name | Client → path → Server | Placement | Network | Expected | Requires OVN-K? | Other requirements | Test types | Additional resources |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 32 / `POD_TO_POD_ANP_ALLOW` | Pod → IP + ANP allow → Pod | different | default | passes | ANP support | ANP CRD + enforcement | policy | ANP |
| 33 / `POD_TO_POD_ANP_DENY` | Pod → IP + ANP deny → Pod | different | default | blocked | ANP support | ANP CRD + enforcement | policy | ANP |
| 34 / `POD_TO_POD_ANP_PASS_NP_DENY` | Pod → IP + ANP Pass + NP deny → Pod | different | default | blocked | ANP support | ANP CRD + enforcement; NP enforcement | policy | ANP; NP |
| 35 / `POD_TO_POD_NP_DENY` | Pod → IP + NP deny → Pod | different | default | blocked | no | NP enforcement | policy | NP |
| 36 / `POD_TO_POD_NP_ALLOW` | Pod → IP + NP allow → Pod | different | default | passes | no | NP enforcement | policy | NP |
| 69 / `HOST_TO_POD_NP_NS_SELECTOR_ALLOW` | Host-network pod → IP + namespaceSelector NP → Pod | different | default | passes | no | NP enforcement; host-network namespace labels | policy | NP |

## Primary UDN

The client uses the primary UDN. Cases 80–81 target a server on the cluster default
network and expect isolation. Cases 100–101 use server-node NodePort access.

`TFT_ENABLE_TARGET_ACCESS_SUBTESTS=true` adds `IP` and `SERVICE_NAME` targets. NodePort
also retains its named node-IP target. `TFT_DEFAULT_TARGET_ACCESS_MODE` changes only the
normal ClusterIP/LoadBalancer target; NodePort selection comes from the case ID.

| ID / Name | Client → path → Server | Placement | Network | Expected | Requires OVN-K? | Other requirements | Test types | Additional resources |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 37 / `UDN_PRIMARY_POD_TO_POD_SAME_NODE` | Pod → direct IP → Pod | same | primary UDN/CUDN | passes | yes | UDN/CUDN API | general + RDMA* | primary UDN/CUDN |
| 38 / `UDN_PRIMARY_POD_TO_POD_DIFF_NODE` | Pod → direct IP → Pod | different | primary UDN/CUDN | passes | yes | UDN/CUDN API | general + RDMA* | primary UDN/CUDN |
| 39 / `UDN_PRIMARY_POD_TO_CLUSTER_IP_TO_POD_SAME_NODE` | Pod → ClusterIP Service → Pod | same | primary UDN/CUDN | passes | yes | UDN/CUDN API | general + RDMA* | primary UDN/CUDN; Service |
| 40 / `UDN_PRIMARY_POD_TO_CLUSTER_IP_TO_POD_DIFF_NODE` | Pod → ClusterIP Service → Pod | different | primary UDN/CUDN | passes | yes | UDN/CUDN API | general + RDMA* | primary UDN/CUDN; Service |
| 41 / `UDN_PRIMARY_POD_TO_NODE_PORT_TO_POD_SAME_NODE` | Pod → client-node NodePort → Pod | same | primary UDN/CUDN | passes | yes | UDN/CUDN API | general + RDMA* | primary UDN/CUDN; Service |
| 42 / `UDN_PRIMARY_POD_TO_NODE_PORT_TO_POD_DIFF_NODE` | Pod → client-node NodePort → Pod | different | primary UDN/CUDN | passes | yes | UDN/CUDN API | general + RDMA* | primary UDN/CUDN; Service |
| 43 / `UDN_PRIMARY_POD_TO_EXTERNAL` | Pod → external endpoint → External server | external | primary UDN/CUDN | passes | yes | UDN/CUDN API; external tool server or Podman; HTTP URL optional | general + RDMA* | primary UDN/CUDN |
| 44 / `UDN_PRIMARY_POD_TO_POD_NP_DENY` | Pod → IP + NP deny → Pod | different | primary UDN/CUDN | blocked | yes | UDN/CUDN API; NP enforcement | policy | primary UDN/CUDN; NP |
| 45 / `UDN_PRIMARY_POD_TO_POD_NP_ALLOW` | Pod → IP + NP allow → Pod | different | primary UDN/CUDN | passes | yes | UDN/CUDN API; NP enforcement | policy | primary UDN/CUDN; NP |
| 46 / `UDN_PRIMARY_POD_TO_LOAD_BALANCER_TO_POD_SAME_NODE` | Pod → LoadBalancer Service → Pod | same | primary UDN/CUDN | passes | yes | UDN/CUDN API; LB provider + reachable IP | general + RDMA* | primary UDN/CUDN; Service |
| 47 / `UDN_PRIMARY_POD_TO_LOAD_BALANCER_TO_POD_DIFF_NODE` | Pod → LoadBalancer Service → Pod | different | primary UDN/CUDN | passes | yes | UDN/CUDN API; LB provider + reachable IP | general + RDMA* | primary UDN/CUDN; Service |
| 80 / `UDN_PRIMARY_POD_TO_CDN_POD_SAME_NODE` | Pod → direct IP → Pod (CDN) | same | primary UDN/CUDN | blocked | yes | UDN/CUDN API | policy | primary UDN/CUDN |
| 81 / `UDN_PRIMARY_POD_TO_CDN_POD_DIFF_NODE` | Pod → direct IP → Pod (CDN) | different | primary UDN/CUDN | blocked | yes | UDN/CUDN API | policy | primary UDN/CUDN |
| 100 / `UDN_PRIMARY_POD_TO_NODE_PORT_SERVER_TO_POD_SAME_NODE` | Pod → server-node NodePort → Pod | same | primary UDN/CUDN | passes | yes | UDN/CUDN API | general + RDMA* | primary UDN/CUDN; Service |
| 101 / `UDN_PRIMARY_POD_TO_NODE_PORT_SERVER_TO_POD_DIFF_NODE` | Pod → server-node NodePort → Pod | different | primary UDN/CUDN | passes | yes | UDN/CUDN API | general + RDMA* | primary UDN/CUDN; Service |

## Secondary UDN and CUDN

The case fixes the UDN/CUDN mode, topology and generated NAD. Cases 82–91 add
different-node MNP enforcement; localnet also needs the physical-network bridge mapping.

| ID / Name | Client → path → Server | Placement | Network | Expected | Requires OVN-K? | Other requirements | Test types | Additional resources |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 70 / `CUDN_LAYER3_POD_TO_POD_SAME_NODE` | Pod → second-interface IP → Pod | same | CUDN layer3 | passes | yes | UDN/CUDN API | general + RDMA* | CUDN + generated NAD |
| 71 / `CUDN_LAYER3_POD_TO_POD_DIFF_NODE` | Pod → second-interface IP → Pod | different | CUDN layer3 | passes | yes | UDN/CUDN API | general + RDMA* | CUDN + generated NAD |
| 72 / `UDN_LAYER3_POD_TO_POD_SAME_NODE` | Pod → second-interface IP → Pod | same | UDN layer3 | passes | yes | UDN/CUDN API | general + RDMA* | UDN + generated NAD |
| 73 / `UDN_LAYER3_POD_TO_POD_DIFF_NODE` | Pod → second-interface IP → Pod | different | UDN layer3 | passes | yes | UDN/CUDN API | general + RDMA* | UDN + generated NAD |
| 74 / `CUDN_LAYER2_POD_TO_POD_SAME_NODE` | Pod → second-interface IP → Pod | same | CUDN layer2 | passes | yes | UDN/CUDN API | general + RDMA* | CUDN + generated NAD |
| 75 / `CUDN_LAYER2_POD_TO_POD_DIFF_NODE` | Pod → second-interface IP → Pod | different | CUDN layer2 | passes | yes | UDN/CUDN API | general + RDMA* | CUDN + generated NAD |
| 76 / `UDN_LAYER2_POD_TO_POD_SAME_NODE` | Pod → second-interface IP → Pod | same | UDN layer2 | passes | yes | UDN/CUDN API | general + RDMA* | UDN + generated NAD |
| 77 / `UDN_LAYER2_POD_TO_POD_DIFF_NODE` | Pod → second-interface IP → Pod | different | UDN layer2 | passes | yes | UDN/CUDN API | general + RDMA* | UDN + generated NAD |
| 78 / `CUDN_LOCALNET_POD_TO_POD_SAME_NODE` | Pod → second-interface IP → Pod | same | CUDN localnet | passes | yes | UDN/CUDN API; bridge mapping | general + RDMA* | CUDN + generated NAD |
| 79 / `CUDN_LOCALNET_POD_TO_POD_DIFF_NODE` | Pod → second-interface IP → Pod | different | CUDN localnet | passes | yes | UDN/CUDN API; bridge mapping | general + RDMA* | CUDN + generated NAD |
| 82 / `CUDN_LAYER3_POD_TO_POD_MNP_DENY` | Pod → second-interface IP + MNP deny → Pod | different | CUDN layer3 | blocked | yes | UDN/CUDN API; MNP CRD + enforcement | policy | CUDN + generated NAD; MNP |
| 83 / `CUDN_LAYER3_POD_TO_POD_MNP_ALLOW` | Pod → second-interface IP + MNP allow → Pod | different | CUDN layer3 | passes | yes | UDN/CUDN API; MNP CRD + enforcement | policy | CUDN + generated NAD; MNP |
| 84 / `UDN_LAYER3_POD_TO_POD_MNP_DENY` | Pod → second-interface IP + MNP deny → Pod | different | UDN layer3 | blocked | yes | UDN/CUDN API; MNP CRD + enforcement | policy | UDN + generated NAD; MNP |
| 85 / `UDN_LAYER3_POD_TO_POD_MNP_ALLOW` | Pod → second-interface IP + MNP allow → Pod | different | UDN layer3 | passes | yes | UDN/CUDN API; MNP CRD + enforcement | policy | UDN + generated NAD; MNP |
| 86 / `CUDN_LAYER2_POD_TO_POD_MNP_DENY` | Pod → second-interface IP + MNP deny → Pod | different | CUDN layer2 | blocked | yes | UDN/CUDN API; MNP CRD + enforcement | policy | CUDN + generated NAD; MNP |
| 87 / `CUDN_LAYER2_POD_TO_POD_MNP_ALLOW` | Pod → second-interface IP + MNP allow → Pod | different | CUDN layer2 | passes | yes | UDN/CUDN API; MNP CRD + enforcement | policy | CUDN + generated NAD; MNP |
| 88 / `UDN_LAYER2_POD_TO_POD_MNP_DENY` | Pod → second-interface IP + MNP deny → Pod | different | UDN layer2 | blocked | yes | UDN/CUDN API; MNP CRD + enforcement | policy | UDN + generated NAD; MNP |
| 89 / `UDN_LAYER2_POD_TO_POD_MNP_ALLOW` | Pod → second-interface IP + MNP allow → Pod | different | UDN layer2 | passes | yes | UDN/CUDN API; MNP CRD + enforcement | policy | UDN + generated NAD; MNP |
| 90 / `CUDN_LOCALNET_POD_TO_POD_MNP_DENY` | Pod → second-interface IP + MNP deny → Pod | different | CUDN localnet | blocked | yes | UDN/CUDN API; bridge mapping; MNP CRD + enforcement | policy | CUDN + generated NAD; MNP |
| 91 / `CUDN_LOCALNET_POD_TO_POD_MNP_ALLOW` | Pod → second-interface IP + MNP allow → Pod | different | CUDN localnet | passes | yes | UDN/CUDN API; bridge mapping; MNP CRD + enforcement | policy | CUDN + generated NAD; MNP |

## Selecting test cases

`test_cases` accepts IDs, enum names, comma-separated selections, inclusive ranges, YAML
lists, and `"*"`. Quote `"*"` so YAML does not interpret an alias. Examples:
`"1,2,POD_TO_HOST_SAME_NODE,6"`, `"1-4,60-67"`, `"POD_TO_POD_SAME_NODE-4"`, or `[1,
POD_TO_POD_DIFF_NODE]`.

Every connection runs the **union** of TFT-level and connection-level cases, with
duplicates removed. TFT-level cases are first; additional connection cases follow.
Shared network setup uses the union of all effective connection lists. With
`pre_provision: true`, cases are still provisioned only for connections that select
them.

| Selection value | `tft[].test_cases` | `connections[].test_cases` | `plugins[].test_cases` |
| --- | --- | --- | --- |
| Omitted or `null` | All cases | No additions | Every selected case |
| Empty string `""` | All cases | All cases | All cases |
| Empty list `[]` | No shared cases | No additions | Never run this plugin |
| `"*"` | All cases | All cases | All cases |
| IDs, names, ranges, list | Shared selected cases | Add selected cases | Restrict to these selected cases |

To choose entirely per connection, set `tft[].test_cases: []`. Omitting that field
selects all cases, including those requiring specialized infrastructure. A plugin's
selection filters when it runs; it does not add traffic cases.

```yaml
tft:
  - name: Mixed traffic tools
    namespace: tft-docs
    test_cases: "1-4"
    duration: 10
    connections:
      - name: iperf
        type: iperf-tcp
        test_cases: "5"
        server:
          - name: worker-1
        client:
          - name: worker-2
      - name: http
        type: http
        server:
          - name: worker-1
            pod_port: 5202
            host_port: 5302
        client:
          - name: worker-2
```
 The iperf connection runs cases 1–5; HTTP runs 1–4. Adding HTTP `test_cases: "2,6"`
runs 1–4 and 6, with case 2 only once. Give connections unique server ports to avoid
shared pod or Service conflicts.

## Starter suites

| Suite | Cases | Requirements |
| --- | --- | --- |
| Smoke | `1,2,5,6` | Two workers and ordinary pod/Service networking; [example](../../examples/configs/smoke.yaml). |
| Default network | `1-24,92-99` | Pod/host connectivity and NodePort access; [example](../../examples/configs/full-default-network.yaml). Add `60-67` only with a LoadBalancer provider and `25,26` with an external server. |
| NetworkPolicy | `35,36` | CNI with NetworkPolicy enforcement; add `69` when host-network namespace selection is supported. |
| AdminNetworkPolicy | `32-34` | ANP CRD and enforcement. |
| Primary UDN | `37,38` | OVN-Kubernetes UDN support; expand into Services and policy after connectivity works. |
| Secondary UDN/CUDN | `70-77,82-89` | OVN-Kubernetes secondary network and MNP support; add localnet separately. |

## Next steps

- [Choose test types](test-types.md)
- [Configure a scenario](scenarios/basic-pod-networking.md)
- [Configuration reference](config-reference.md)
