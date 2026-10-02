<!--
SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
SPDX-License-Identifier: Apache-2.0
-->

# Secondary networks and SR-IOV

Use this page when you need a second interface or a VF-backed default attachment.

## Cluster prerequisites

A pre-existing NAD may use another CNI; OVN-Kubernetes is required for the NAD TFT
generates when one is missing. That template uses `type: ovn-k8s-cni-overlay`, not a
generic secondary CNI. For a non-OVN network, pre-create your NAD and set both
per-endpoint references as in this example. It also avoids asking TFT to create a NAD in
the test namespace.

For cases 27–31, endpoint `secondary_network_nad` overrides the connection setting.
Connection-level `secondary_network_nad` otherwise defaults to `tft-secondary`; missing
regular NADs may be generated using `TFT_SECONDARY_NAD_SUBNETS`,
`TFT_SECONDARY_NAD_MTU`, and `TFT_SECONDARY_NAD_TOPOLOGY`. The current creation logic
checks the test namespace; use explicit per-endpoint references for NADs in other
namespaces.

Check access and the relevant prerequisites with these read-only commands. Replace
site-specific names and paths before running:

```bash
kubectl get crd network-attachment-definitions.k8s.cni.cncf.io
kubectl -n default get network-attachment-definition secondary -o yaml
```

## Minimal configuration

Copy
[examples/configs/secondary-networks-sriov.yaml](../../../examples/configs/secondary-networks-sriov.yaml),
replace its placeholders, and run from the repository root with the tenant kubeconfig
explicitly supplied:

```bash
./tft.py --check --kubeconfig /path/tenant.yaml examples/configs/secondary-networks-sriov.yaml
```

```yaml
tft:
- name: Secondary networks and SR-IOV
  namespace: tft-docs
  test_cases: 27,28
  duration: 10
  connections:
  - name: traffic
    type: iperf-tcp
    instances: 1
    reverse: false
    server:
    - name: worker-1
      secondary_network_nad: default/secondary
    client:
    - name: worker-2
      secondary_network_nad: default/secondary
```

## Relevant cases and fields

This example selects `27,28`. See the [catalog](../test-cases.md) for path and placement
and the [configuration reference](../config-reference.md) for field defaults and
precedence.

## Resources and cleanup

Traffic pods with secondary attachments. A missing connection-level regular NAD may be
created and later deleted if TFT-labelled; pre-existing unlabelled NADs remain.

Use a dedicated namespace. Existing namespace labels can be changed; see [cleanup
behavior](../../getting-started/running-tests.md#namespaces-and-cleanup).

## Expected results

Cases 27 and 28 pass over the second interface, locally and across nodes. Inspect the
Multus network-status annotation and generated client destination when confirming the
path.

Inspect the recorded JSON with `./print_results.py --no-color RESULT.json`. `--check`
makes recorded flow/plugin failures fail the runner command.

## Variations

For a VF-backed **default** attachment, select ordinary pod cases such as `1,2`, set
`sriov: true` on server and client, and set `default_network` to an existing SR-IOV NAD.
Configure the SR-IOV operator/device plugin (or equivalent), usable VFs, and matching
extended-resource allocation. `resource_name` on the connection can be explicit or
detected from the NAD's `k8s.v1.cni.cncf.io/resourceName` annotation. TFT creates
workloads, not VFs, operators, or SR-IOV network policy objects. Add the matching
offload plugin only when representors and offload support are available.

Cases 70–79 and 82–91 use case-generated UDN/CUDN NADs rather than the regular
`secondary_network_nad` option. MultiNetworkPolicy cases 29–31 are described in the
policy page.

## Next steps

- [Configuration reference](../config-reference.md)
- [Environment variables](../environment-variables.md)
- [Reading results](../../results/reading-results.md)
