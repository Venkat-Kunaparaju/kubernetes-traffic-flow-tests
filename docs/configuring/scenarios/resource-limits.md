<!--
SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
SPDX-License-Identifier: Apache-2.0
-->

# Traffic-pod resource limits

Use this page when you want throughput under explicit CPU or memory budgets.

## Cluster prerequisites

OVN-Kubernetes is not required. The connection-level `cpu_request`, `cpu_limit`,
`mem_request`, and `mem_limit` apply to both traffic endpoints. Omitted values set no
request/limit. Kubernetes, rather than TFT, validates quantity syntax and whether
requests fit the node. CPU throttling can reduce throughput; memory limits can terminate
a container.

Check access and the relevant prerequisites with these read-only commands. Replace
site-specific names and paths before running:

```bash
kubectl get nodes -o wide
kubectl describe node worker-1
```

## Minimal configuration

Copy
[examples/configs/resource-limits.yaml](../../../examples/configs/resource-limits.yaml),
replace its placeholders, and run from the repository root with the tenant kubeconfig
explicitly supplied:

```bash
./tft.py --check --kubeconfig /path/tenant.yaml examples/configs/resource-limits.yaml
```

```yaml
tft:
- name: Traffic-pod resource limits
  namespace: tft-docs
  test_cases: 1,2,5,6
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
    cpu_request: 100m
    cpu_limit: 1000m
    mem_request: 64Mi
    mem_limit: 256Mi
```

## Relevant cases and fields

This example selects `1,2,5,6`. See the [catalog](../test-cases.md) for path and
placement and the [configuration reference](../config-reference.md) for field defaults
and precedence.

## Resources and cleanup

Traffic pods with the configured requests and limits; Service cases also create
Services. Ordinary cleanup removes them.

Use a dedicated namespace. Existing namespace labels can be changed; see [cleanup
behavior](../../getting-started/running-tests.md#namespaces-and-cleanup).

## Expected results

Pods schedule, the configured resources appear in pod specs/rendered manifests, and all
selected traffic flows pass. Compare throughput to a baseline captured with the same
resource budgets.

Inspect the recorded JSON with `./print_results.py --no-color RESULT.json`. `--check`
makes recorded flow/plugin failures fail the runner command.

## Variations

Add `plugins: [measure_cpu]` to record endpoint CPU idleness. The plugin does not
enforce a CPU limit. `pre_provision: true` and server `persistent: true` are useful for
repeated comparisons but do not prevent final cleanup.

The existing CI [resource-limits
config](../../../.github/tft-configs/resource-limits.yaml) combines these fields with
SR-IOV, MNP, and DPU simulation settings. The example here isolates the resource-budget
settings so those extra prerequisites are not needed.

## Next steps

- [Configuration reference](../config-reference.md)
- [Environment variables](../environment-variables.md)
- [Reading results](../../results/reading-results.md)
