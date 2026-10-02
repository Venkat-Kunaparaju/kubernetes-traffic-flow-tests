<!--
SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
SPDX-License-Identifier: Apache-2.0
-->

# AdminNetworkPolicy

Use this page when you need cluster-level Allow, Deny, and Pass checks.

## Cluster prerequisites

These tests target the OVN-Kubernetes ANP workflow. AdminNetworkPolicy is a standard
API, so another CNI can be usable if it implements the same behavior; API presence alone
does not establish enforcement. Cluster-scoped ANP create and delete permission is
required.

| Case | Action | Expected traffic |
| --- | --- | --- |
| 32 | Allow | Passes |
| 33 | Deny | Blocked |
| 34 | Pass, followed by NetworkPolicy deny | Blocked |

TFT creates an ANP at priority 50 with ingress and egress rules selecting test pods.
Existing administrator policies may affect these flows; inspect them before running on a
shared cluster. Pass delegates the decision to namespace-scoped NetworkPolicy rather
than explicitly allowing traffic.

Check access and the relevant prerequisites with these read-only commands. Replace
site-specific names and paths before running:

```bash
kubectl get crd adminnetworkpolicies.policy.networking.k8s.io
kubectl get adminnetworkpolicies
```

## Minimal configuration

Copy
[examples/configs/admin-network-policy.yaml](../../../examples/configs/admin-network-policy.yaml),
replace its placeholders, and run from the repository root with the tenant kubeconfig
explicitly supplied:

```bash
./tft.py --check --kubeconfig /path/tenant.yaml examples/configs/admin-network-policy.yaml
```

```yaml
tft:
- name: AdminNetworkPolicy
  namespace: tft-docs
  test_cases: 32-34
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

This example selects `32-34`. See the [catalog](../test-cases.md) for path and placement
and the [configuration reference](../config-reference.md) for field defaults and
precedence.

## Resources and cleanup

Traffic pods, a labelled cluster-scoped AdminNetworkPolicy, and a deny NetworkPolicy for
case 34. Normal cleanup removes the TFT-labelled policy resources.

Use a dedicated namespace. Existing namespace labels can be changed; see [cleanup
behavior](../../getting-started/running-tests.md#namespaces-and-cleanup).

## Expected results

All three results should be marked succeeded: traffic flows in case 32 and fails to
connect in cases 33/34. The passing flow establishes that the network works before the
deny checks.

Inspect the recorded JSON with `./print_results.py --no-color RESULT.json`. `--check`
makes recorded flow/plugin failures fail the runner command.

## Variations

Use `type: iperf-tcp` for throughput plus policy checks. Limit plugins to the positive
case if measurements require flowing traffic. Avoid interpreting unavailable deny-case
bitrate as a performance regression.

## Next steps

- [Configuration reference](../config-reference.md)
- [Environment variables](../environment-variables.md)
- [Reading results](../../results/reading-results.md)
