<!--
SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
SPDX-License-Identifier: Apache-2.0
-->

# EgressIP

Use this page when you need to verify the source address observed by an external server.

## Cluster prerequisites

OVN-Kubernetes is required. Pick an unused assignable EgressIP within the egress node's
`k8s.ovn.org/host-cidrs`; `192.0.2.100` below is a documentation placeholder, not an
address to copy into a live cluster. Replace `node` and `ip` for your site. If `node` is
omitted, the configured client node is used.

Use an iperf TCP connection for this example because source validation extracts
`remote_host` from the server's captured iperf JSON. Other tool handlers do not supply
that same server result. The default external path uses a runner-side Podman server; the
runner address and published port must be reachable from the cluster. This recipe
requires that captured-server-output path. A pre-existing `TFT_EXTERNAL_SERVER` is
rejected for EgressIP because TFT cannot capture its server output; unset that variable
for this recipe.

Check access and the relevant prerequisites with these read-only commands. Replace
site-specific names and paths before running:

```bash
kubectl get crd egressips.k8s.ovn.org
kubectl get egressips
podman --version
kubectl get node worker-2 -o jsonpath='{.metadata.annotations.k8s\.ovn\.org/host-cidrs}'
```

## Minimal configuration

Copy [examples/configs/egress-ip.yaml](../../../examples/configs/egress-ip.yaml),
replace its placeholders, and run from the repository root with the tenant kubeconfig
explicitly supplied:

```bash
./tft.py --check --kubeconfig /path/tenant.yaml examples/configs/egress-ip.yaml
```

```yaml
tft:
- name: EgressIP
  namespace: tft-docs
  test_cases: '68'
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
    egress_ip:
      ip: 192.0.2.100
      node: worker-2
```

## Relevant cases and fields

This example selects `68`. See the [catalog](../test-cases.md) for path and placement
and the [configuration reference](../config-reference.md) for field defaults and
precedence.

## Resources and cleanup

An EgressIP custom resource scoped by selector to the test namespace, an
egress-assignable node label, the client pod, and the runner-side external server.
Cleanup removes labelled EgressIP resources and TFT-tracked egress node labels.

Use a dedicated namespace. Existing namespace labels can be changed; see [cleanup
behavior](../../getting-started/running-tests.md#namespaces-and-cleanup).

## Expected results

TFT polls up to 120 seconds for IP assignment, runs traffic, and compares the observed
server-side client source IP to the configured EgressIP. Both connectivity and the
source-address check must succeed.

Inspect the recorded JSON with `./print_results.py --no-color RESULT.json`. `--check`
makes recorded flow/plugin failures fail the runner command.

## Variations

Omit `egress_ip.node` to use the client node. Check the selected node annotation and
EgressIP status before investigating a source mismatch. This case is separate from
ordinary external connectivity cases 25/26.

## Next steps

- [Configuration reference](../config-reference.md)
- [Environment variables](../environment-variables.md)
- [Reading results](../../results/reading-results.md)
