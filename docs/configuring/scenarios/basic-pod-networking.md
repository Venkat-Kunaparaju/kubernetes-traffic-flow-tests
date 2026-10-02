<!--
SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
SPDX-License-Identifier: Apache-2.0
-->

# Basic pod networking

Use this page when you want to compare pod and host connectivity.

## Cluster prerequisites

OVN-Kubernetes is not required. Cases 1–4 compare pod-to-pod and pod-to-host; 13–16
compare host-to-host and host-to-pod. A host endpoint is a host-network pod, not a shell
command executed directly on the node. Use cases 1, 3, 13, and 15 on a single worker;
same-node execution anchors both endpoints to the server node.

Check access and the relevant prerequisites with these read-only commands. Replace
site-specific names and paths before running:

```bash
kubectl get nodes -o wide
```

## Minimal configuration

Copy
[examples/configs/basic-pod-networking.yaml](../../../examples/configs/basic-pod-networking.yaml),
replace its placeholders, and run from the repository root with the tenant kubeconfig
explicitly supplied:

```bash
./tft.py --check --kubeconfig /path/tenant.yaml examples/configs/basic-pod-networking.yaml
```

```yaml
tft:
- name: Basic pod networking
  namespace: tft-docs
  test_cases: 1-4,13-16
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

This example selects `1-4,13-16`. See the [catalog](../test-cases.md) for path and
placement and the [configuration reference](../config-reference.md) for field defaults
and precedence.

## Resources and cleanup

Traffic pods only, plus the test namespace if missing. Host-backed endpoints use
hostNetwork.

Use a dedicated namespace. Existing namespace labels can be changed; see [cleanup
behavior](../../getting-started/running-tests.md#namespaces-and-cleanup).

## Expected results

Every selected flow should succeed; iperf reports TX/RX Gbit/s. Compare case 1 to 2 and
case 3 to 4 to separate local and cross-node paths.

Inspect the recorded JSON with `./print_results.py --no-color RESULT.json`. `--check`
makes recorded flow/plugin failures fail the runner command.

## Variations

Start with the smoke suite (1,2,5,6); set `reverse: true` to add iperf TCP reverse
traffic. Select only case 1 for the shortest same-node check.

## Next steps

- [Configuration reference](../config-reference.md)
- [Environment variables](../environment-variables.md)
- [Reading results](../../results/reading-results.md)
