<!--
SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
SPDX-License-Identifier: Apache-2.0
-->

# Service traffic

Use this page when you want ClusterIP, NodePort, or LoadBalancer coverage.

## Cluster prerequisites

OVN-Kubernetes is not required for the default-network cases. Cases 5–8 and 17–20 use
ClusterIP; 9–12 and 21–24 use client-node NodePort; 92–99 use server-node NodePort;
60–67 use LoadBalancer. The configured endpoints' placement is independent of which node
IP is used as the NodePort target.

For LoadBalancer, install/configure a provider such as MetalLB and ensure its allocated
IPs are reachable from the traffic clients. TFT creates Services; it does not install a
LoadBalancer implementation or allocate the provider's address pools.

Check access and the relevant prerequisites with these read-only commands. Replace
site-specific names and paths before running:

```bash
kubectl get nodes -o wide
kubectl get services -A
```

## Minimal configuration

Copy [examples/configs/services.yaml](../../../examples/configs/services.yaml), replace
its placeholders, and run from the repository root with the tenant kubeconfig explicitly
supplied:

```bash
./tft.py --check --kubeconfig /path/tenant.yaml examples/configs/services.yaml
```

```yaml
tft:
- name: Service traffic
  namespace: tft-docs
  test_cases: 5,6,9,10,92,93
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

This example selects `5,6,9,10,92,93`. See the [catalog](../test-cases.md) for path and
placement and the [configuration reference](../config-reference.md) for field defaults
and precedence.

## Resources and cleanup

Traffic pods and labelled Services in the test namespace. LoadBalancer Services have
their own TFT service-type label and are cleaned after use.

Use a dedicated namespace. Existing namespace labels can be changed; see [cleanup
behavior](../../getting-started/running-tests.md#namespaces-and-cleanup).

## Expected results

Selected flows should succeed. A LoadBalancer case needs an assigned external IP; a
Pending Service is a provisioning problem before throughput can be tested.

Inspect the recorded JSON with `./print_results.py --no-color RESULT.json`. `--check`
makes recorded flow/plugin failures fail the runner command.

## Variations

Select `60,61` once LoadBalancer provisioning works. Enable additional Service IP/DNS
variants with `TFT_ENABLE_TARGET_ACCESS_SUBTESTS=true`; ClusterIP and LoadBalancer run
two targets and NodePort three. The `IP` variant on a NodePort Service uses its
ClusterIP; it does not replace the named node-IP subtest. `SERVICE_NAME` requires
working cluster DNS. `TFT_DEFAULT_TARGET_ACCESS_MODE` accepts `IP` and `SERVICE_NAME`
for ClusterIP/LoadBalancer only.

## Next steps

- [Configuration reference](../config-reference.md)
- [Environment variables](../environment-variables.md)
- [Reading results](../../results/reading-results.md)
