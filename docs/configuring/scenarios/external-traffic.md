<!--
SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
SPDX-License-Identifier: Apache-2.0
-->

# External traffic

Use this page when you need pod or host egress to an endpoint outside Kubernetes.

## Cluster prerequisites

OVN-Kubernetes is not required for cases 25–26. The default path starts an external
server with Podman on the TFT runner and publishes a dynamically assigned host port.
Routing and firewalls must let cluster clients reach that runner address and port. TFT
does not install Podman or create firewall rules. Skip the Podman prerequisite when
using an explicitly configured external server.

For a pre-existing iperf3 server, set `TFT_EXTERNAL_SERVER=192.0.2.10:5201` before
running. Replace this documentation address with a reachable host. The server must match
`type`; netperf and HTTP require their respective servers, not an iperf3 listener. If
the port is omitted, TFT uses server `pod_port`. IPv6 endpoint syntax is
`[fd00::1]:5201`.

Check access and the relevant prerequisites with these read-only commands. Replace
site-specific names and paths before running:

```bash
kubectl get nodes -o wide
podman --version
```

## Minimal configuration

Copy
[examples/configs/external-traffic.yaml](../../../examples/configs/external-traffic.yaml),
replace its placeholders, and run from the repository root with the tenant kubeconfig
explicitly supplied:

```bash
./tft.py --check --kubeconfig /path/tenant.yaml examples/configs/external-traffic.yaml
```

```yaml
tft:
- name: External traffic
  namespace: tft-docs
  test_cases: 25,26
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

This example selects `25,26`. See the [catalog](../test-cases.md) for path and placement
and the [configuration reference](../config-reference.md) for field defaults and
precedence.

## Resources and cleanup

Client traffic pods and, on the default path, a per-run Podman server on the runner. TFT
does not delete a pre-existing external server.

Use a dedicated namespace. Existing namespace labels can be changed; see [cleanup
behavior](../../getting-started/running-tests.md#namespaces-and-cleanup).

## Expected results

Each selected external flow succeeds. Check the generated command for the real
destination and published port; iperf throughput measures the entire path to the
external host.

Inspect the recorded JSON with `./print_results.py --no-color RESULT.json`. `--check`
makes recorded flow/plugin failures fail the runner command.

## Variations

For HTTP connectivity, change `type` to `http`, set
`TFT_EXTERNAL_URL=https://example.com/health` to your endpoint, and set
`TFT_EXTERNAL_SERVER_STRING=healthy` to text in its response. The default substring is
`The document has moved`, which may not fit your endpoint. This URL path skips the
Podman server and checks the body substring; it is not a throughput measurement. Primary
UDN external case 43 also uses EXTERNAL_IP mode and can use the same HTTP URL path.

## Next steps

- [Configuration reference](../config-reference.md)
- [Environment variables](../environment-variables.md)
- [Reading results](../../results/reading-results.md)
