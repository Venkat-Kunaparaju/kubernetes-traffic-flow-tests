<!--
SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
SPDX-License-Identifier: Apache-2.0
-->

# NetworkPolicy and MultiNetworkPolicy

Use this page when you need positive and negative policy checks.

## Cluster prerequisites

Ordinary NetworkPolicy is not OVN-specific; the CNI must enforce it. A cluster can
expose the API without enforcing traffic rules. Cases 35/36 cover deny/allow. Use a
matching allow test to demonstrate that endpoints and routing work before interpreting a
denied connection.

MultiNetworkPolicy cases 29–31 additionally require Multus, a usable NAD, the
`multi-networkpolicies.k8s.cni.cncf.io` CRD, and enforcement by the network
implementation. TFT's generated NAD is OVN-specific; use pre-existing per-node NADs for
a different CNI. Verify the CRD with `kubectl get crd
multi-networkpolicies.k8s.cni.cncf.io`.

Check access and the relevant prerequisites with these read-only commands. Replace
site-specific names and paths before running:

```bash
kubectl get nodes -o wide
kubectl get networkpolicies -A
```

## Minimal configuration

Copy
[examples/configs/network-policy.yaml](../../../examples/configs/network-policy.yaml),
replace its placeholders, and run from the repository root with the tenant kubeconfig
explicitly supplied:

```bash
./tft.py --check --kubeconfig /path/tenant.yaml examples/configs/network-policy.yaml
```

```yaml
tft:
- name: NetworkPolicy and MultiNetworkPolicy
  namespace: tft-docs
  test_cases: 35,36
  duration: 10
  connections:
  - name: traffic
    type: http
    instances: 1
    reverse: false
    server:
    - name: worker-1
    client:
    - name: worker-2
```

## Relevant cases and fields

This example selects `35,36`. See the [catalog](../test-cases.md) for path and placement
and the [configuration reference](../config-reference.md) for field defaults and
precedence.

## Resources and cleanup

Traffic pods and labelled NetworkPolicies; MNP cases also create labelled
MultiNetworkPolicies and may create a regular NAD. Policies are cleaned between
applicable cases and at final cleanup.

Use a dedicated namespace. Existing namespace labels can be changed; see [cleanup
behavior](../../getting-started/running-tests.md#namespaces-and-cleanup).

## Expected results

Case 35 passes when HTTP cannot connect; case 36 passes when it can. Cases 29/30 expect
secondary traffic allowed/blocked. Case 31 expects primary traffic to pass while a deny
targets the second interface.

Inspect the recorded JSON with `./print_results.py --no-color RESULT.json`. `--check`
makes recorded flow/plugin failures fail the runner command.

## Variations

Select `29-31` and copy the per-endpoint NAD references from [secondary
networks](secondary-networks-sriov.md). Select `69` to check a namespaceSelector allow
for a host-network client. Its target namespace defaults to `ovn-host-network`; on
OpenShift use `TFT_HOST_NETWORK_NAMESPACE=openshift-host-network`. Host-network policy
behavior and namespace labelling depend on your CNI. Secondary UDN/CUDN MNP cases are
82–91 and require OVN-Kubernetes.

## Next steps

- [Configuration reference](../config-reference.md)
- [Environment variables](../environment-variables.md)
- [Reading results](../../results/reading-results.md)
