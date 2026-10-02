<!--
SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
SPDX-License-Identifier: Apache-2.0
-->

# DPU mode and offload

Use this page when you need paired tenant/infra clusters and representor validation on the DPU.

## Cluster prerequisites

This workflow requires an existing DPU/offload deployment (for example one managed by
the site's DPU operator), OVN-Kubernetes, usable VF attachments, and access to both
clusters. TFT does not install the operator or configure host NICs. `server` and
`client` name **tenant host workers**, not the infra DPU nodes. Replace the example VF
resource and NAD with those advertised by your cluster.

The top-level `kubeconfig_infra` selects DPU mode. Set the host-pairing label key
through `dpu_node_host_label`; each infra node must have a value naming its
corresponding tenant worker, such as `provisioning.dpu.nvidia.com/host: worker-1`.
Providing an infra kubeconfig does not itself enable an offload plugin: choose it under
`connections[].plugins`.

Check access and the relevant prerequisites with these read-only commands. Replace
site-specific names and paths before running:

```bash
kubectl --kubeconfig /path/tenant.yaml get nodes
kubectl --kubeconfig /path/infra.yaml get nodes -L provisioning.dpu.nvidia.com/host
```

## Minimal configuration

Copy [examples/configs/dpu-mode.yaml](../../../examples/configs/dpu-mode.yaml), replace
its placeholders, and run from the repository root with the tenant kubeconfig explicitly
supplied:

```bash
./tft.py --check --kubeconfig /path/tenant.yaml examples/configs/dpu-mode.yaml --kubeconfig-infra /path/infra.yaml
```

```yaml
kubeconfig: /path/tenant.yaml
kubeconfig_infra: /path/infra.yaml
dpu_node_host_label: provisioning.dpu.nvidia.com/host
tft:
- name: DPU mode and offload
  namespace: tft-docs
  test_cases: 1,2
  duration: 10
  connections:
  - name: traffic
    type: iperf-tcp
    instances: 1
    reverse: false
    server:
    - name: worker-1
      sriov: true
      default_network: default/ovn-primary
    client:
    - name: worker-2
      sriov: true
      default_network: default/ovn-primary
    resource_name: example.com/vf
    plugins:
    - name: validate_offload
      test_cases:
      - 1
      - 2
```

## Relevant cases and fields

This example selects `1,2`. See the [catalog](../test-cases.md) for path and placement
and the [configuration reference](../config-reference.md) for field defaults and
precedence.

## Resources and cleanup

Tenant traffic pods and privileged infra tool pods/namespaces as needed. Only newly
created infra namespaces are recorded for deletion; pre-existing infra namespaces
remain.

Use a dedicated namespace. Existing namespace labels can be changed; see [cleanup
behavior](../../getting-started/running-tests.md#namespaces-and-cleanup).

## Expected results

Traffic passes and each applicable VF-backed endpoint passes its selected offload check.
The plugin maps pod PF/VF indices to a DPU representor via devlink and samples its
statistics around the traffic run.

Inspect the recorded JSON with `./print_results.py --no-color RESULT.json`. `--check`
makes recorded flow/plugin failures fail the runner command.

## Variations

Use `ovs_doca_validate_offload` for OVS-DOCA counters; use `validate_offload` for
`ethtool -S` counters. A delta of 1000 or more RX or TX software packets fails in this
upstream version. Success with a host-backed/external skip message is not VF offload
validation. Add `measure_cpu` to sample paired DPU CPU load; that plugin also requires
the host-pairing label. See [plugins](../plugins.md) for metric fields and privilege
requirements.

Kubeconfig flags or environment variables override the YAML pair as a unit. Supply both
CLI flags when overriding DPU access.

## Next steps

- [Configuration reference](../config-reference.md)
- [Environment variables](../environment-variables.md)
- [Reading results](../../results/reading-results.md)
