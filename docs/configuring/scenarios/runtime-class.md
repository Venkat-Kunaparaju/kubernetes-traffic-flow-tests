<!--
SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
SPDX-License-Identifier: Apache-2.0
-->

# RuntimeClass

Use this page when you want traffic pods to run with Kata or another installed runtime.

## Cluster prerequisites

OVN-Kubernetes is not required. An administrator must install and configure the runtime
handler (for example Kata Containers) and create its RuntimeClass; TFT does not install
either. Set `runtime_class_name` at test level or override it on an individual
server/client endpoint.

Kubernetes combines the RuntimeClass's `scheduling.nodeSelector` with TFT's hostname
selector. Every named node running an eligible traffic pod must match both. TFT checks
that configured RuntimeClasses exist before starting; it does not establish that every
node can schedule them. A mismatch can leave pods Pending with `node(s) didn't match
Pod's node affinity/selector`.

Check access and the relevant prerequisites with these read-only commands. Replace
site-specific names and paths before running:

```bash
kubectl get runtimeclass kata -o yaml
kubectl get nodes --show-labels
```

## Minimal configuration

Copy
[examples/configs/runtime-class.yaml](../../../examples/configs/runtime-class.yaml),
replace its placeholders, and run from the repository root with the tenant kubeconfig
explicitly supplied:

```bash
./tft.py --check --kubeconfig /path/tenant.yaml examples/configs/runtime-class.yaml
```

```yaml
tft:
- name: RuntimeClass
  namespace: tft-docs
  test_cases: 1,2
  duration: 10
  runtime_class_name: kata
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

This example selects `1,2`. See the [catalog](../test-cases.md) for path and placement
and the [configuration reference](../config-reference.md) for field defaults and
precedence.

## Resources and cleanup

Normal, secondary-network, and SR-IOV traffic pods receive runtimeClassName.
Host-network endpoints, Podman servers, infra helper pods, and plugin tool pods use the
cluster default runtime. TFT does not own or delete RuntimeClasses.

Use a dedicated namespace. Existing namespace labels can be changed; see [cleanup
behavior](../../getting-started/running-tests.md#namespaces-and-cleanup).

## Expected results

Eligible traffic pods schedule with the chosen RuntimeClass and the selected flows pass.
With `--no-cleanup` and pre-provisioning, inspect runtimeClassName in the rendered or
live pod YAML.

Inspect the recorded JSON with `./print_results.py --no-color RESULT.json`. `--check`
makes recorded flow/plugin failures fail the runner command.

## Variations

If only one worker supports Kata, select case `1` and use that node for both endpoints.
For different-node pod cases both workers must support it. With a host-network endpoint
only the other, eligible traffic endpoint needs the runtime.

Override a client independently:

```yaml
client:
  - name: worker-2
    runtime_class_name: kata-coldplug
```
 Both configured RuntimeClasses must exist. In DPU mode the endpoint names remain tenant
host workers; infra node names belong to the kubeconfig/pairing workflow, not to
`server` and `client`.

## Next steps

- [Configuration reference](../config-reference.md)
- [Environment variables](../environment-variables.md)
- [Reading results](../../results/reading-results.md)
