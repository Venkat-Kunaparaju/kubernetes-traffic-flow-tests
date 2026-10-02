<!--
SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
SPDX-License-Identifier: Apache-2.0
-->

# UDN and CUDN traffic

Use this page when you need primary-network, secondary-network, or isolation coverage.

## Cluster prerequisites

OVN-Kubernetes is required. UDN is namespace scoped; CUDN selects namespaces from a
cluster-scoped resource. Primary networks replace the pod's default network; secondary
networks provide another interface. In case names, CDN is the **cluster default
network**, distinct from CUDN.

| Cases | Network/path |
| --- | --- |
| 37–42 | Primary pod IP, ClusterIP, and client-node NodePort. |
| 43 | Primary network to external server. |
| 44–45 | Primary NetworkPolicy deny/allow. |
| 46–47 | Primary LoadBalancer to pod. |
| 70–79 | Secondary CUDN L3, UDN L3, CUDN L2, UDN L2, CUDN localnet pairs. |
| 80–81 | Primary UDN client to default-network server; expected isolation. |
| 82–91 | Different-node secondary MNP deny/allow for those five networks. |
| 100–101 | Primary server-node NodePort. |

The primary default is `mode: udn`, `topology: layer3`, `transport: overlay`, `name:
tft-primary`. Use `udn_primary_network` to choose primary CUDN or Layer2. Secondary case
IDs fix their own network type and topology. Their CIDRs and IPAM controls are in the
[environment reference](../environment-variables.md). Layer2/localnet modes other than
unset or `Enabled` omit subnets; TFT does not provide addresses for an IPAM-disabled
network.

Check access and the relevant prerequisites with these read-only commands. Replace
site-specific names and paths before running:

```bash
kubectl get crd userdefinednetworks.k8s.ovn.org clusteruserdefinednetworks.k8s.ovn.org
kubectl get clusteruserdefinednetworks
```

## Minimal configuration

Copy [examples/configs/udn-cudn.yaml](../../../examples/configs/udn-cudn.yaml), replace
its placeholders, and run from the repository root with the tenant kubeconfig explicitly
supplied:

```bash
./tft.py --check --kubeconfig /path/tenant.yaml examples/configs/udn-cudn.yaml
```

```yaml
tft:
- name: UDN and CUDN traffic
  namespace: tft-docs
  test_cases: 37,38
  duration: 10
  connections:
  - name: traffic
    type: iperf-tcp
    instances: 1
    reverse: false
    server:
    - name: worker-1
    client:
    - name: worker-2
```

## Relevant cases and fields

This example selects `37,38`. See the [catalog](../test-cases.md) for path and placement
and the [configuration reference](../config-reference.md) for field defaults and
precedence.

## Resources and cleanup

The `{namespace}-udn` namespace, selected UDN/CUDN resources and workloads, with any
applicable Services or policies. Case 80/81 servers use the base namespace. Newly
created namespaces and labelled network resources are removed at final cleanup.

Use a dedicated namespace. Existing namespace labels can be changed; see [cleanup
behavior](../../getting-started/running-tests.md#namespaces-and-cleanup).

## Expected results

Allow cases pass on their selected network; 80/81 and deny variants report successful
blocking. The primary CIDR defaults to 15.1.0.0/16 with host prefix 24. Ensure test
subnets do not conflict with your environment.

Inspect the recorded JSON with `./print_results.py --no-color RESULT.json`. `--check`
makes recorded flow/plugin failures fail the runner command.

## Variations

### Localnet

Cases 78/79 and 90/91 require a configured physical-network bridge mapping on
participating nodes. Its name defaults to `physnet`, controlled by
`TFT_CUDN_LOCALNET_PHYSICAL_NETWORK`. TFT does not configure host bridges or the
physical underlay. Verify the site mapping before using localnet.

### No-overlay primary CUDN

No-overlay requires primary CUDN Layer3, reachable underlay routes, and the appropriate
OVN-Kubernetes feature support. An unmanaged example is:

```yaml
udn_primary_network:
  name: blue
  mode: cudn
  topology: layer3
  transport: no-overlay
  uplink_name: blue-uplink
  route_advertisement:
    targetVRF: auto
    frr_configuration_selector: {network: blue}
```
 The referenced Uplink and base FRRConfiguration must already exist. Verify with
`kubectl get uplinks` and `kubectl get frrconfigurations -A -l network=blue`. TFT
creates RouteAdvertisements from this selector, not the base FRR deployment or routers.
`name: blue` supplies the CUDN name/VRF relationship; the selector label selects
FRRConfiguration and does not set a VRF name. The selected base configuration needs the
matching router VRF for the `auto` workflow.

Routing defaults to unmanaged. Set `TFT_UDN_NO_OVERLAY_ROUTING_MANAGED=true` for managed
routing and omit `route_advertisement`. With unmanaged routing, omit that mapping only
if routing is provided outside TFT. Omitted `targetVRF` leaves the API default intact;
TFT accepts non-empty strings but the API and FRR configuration determine which values
work. A TransportAccepted wait that times out logs and continues; it is not proof that
the routing works.

### Existing primary CUDN

Set `TFT_EXISTING_PRIMARY_CUDN=blue` and use `mode: cudn` to run against an existing
CUDN with role Primary. TFT creates/reuses the UDN namespace and applies the CUDN's
namespace-selector `matchLabels` to it. `matchExpressions` are rejected. TFT leaves the
supplied CUDN, Uplink, RouteAdvertisements, and base FRRConfiguration unchanged.
Topology, transport, IPAM, and routing are owned by you in this mode; labels applied to
reused namespaces remain afterwards. Existing user-owned network resources must not use
TFT's reserved `tft-tests` label because labelled resources participate in TFT cleanup.
This option does not change secondary CUDN tests.

## Next steps

- [Configuration reference](../config-reference.md)
- [Environment variables](../environment-variables.md)
- [Reading results](../../results/reading-results.md)
