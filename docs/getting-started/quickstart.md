<!--
SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
SPDX-License-Identifier: Apache-2.0
-->

# Run your first test

Use this page when you want a short pod-to-pod connectivity and throughput check.

This walkthrough uses iperf TCP, cases 1 and 2, ten seconds per case, one instance, no
reverse runs, and no plugins. Allow additional time for image pulls and pod startup.
Complete [installation](install.md) first.

## 1. Choose nodes and access

```bash
export TFT_KUBECONFIG="$HOME/.kube/config"
kubectl --kubeconfig "$TFT_KUBECONFIG" get nodes -o wide
```
 Choose two Ready worker nodes and confirm your access permits creation and cleanup of
test resources. For a single-node cluster, select only case `1` and use that node for
both endpoints.

## 2. Copy and edit the configuration

```bash
cp examples/configs/quickstart.yaml quickstart.yaml
```
 Edit `worker-1` and `worker-2` to match the actual node names:

```yaml
tft:
- name: First pod connectivity test
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
    client:
    - name: worker-2
```
 Node names are literal strings. TFT does not expand shell variables in YAML. Same-node
cases put both endpoints on the server's configured node; different-node cases use both
configured nodes. Use a dedicated namespace for the run.

## 3. Run and inspect the result

```bash
./tft.py --check quickstart.yaml
```
 TFT starts the pods, runs traffic, evaluates the results, writes JSON, and cleans up.
The console includes `RESULT: Success = True.`, flow/plugin pass counts, a result-file
path, and a summary. The quickstart normally produces two flow results and zero plugin
results. Throughput depends on your cluster; there is no speed threshold in this first
run. `--check` returns a failing exit status if any recorded flow or plugin fails.

Find the exact path in the `Write results to` log message, or list the files:

```bash
ls -lt ft-logs/*.json
./print_results.py --no-color ft-logs/YOUR-TIMESTAMP.json
```
 Replace `YOUR-TIMESTAMP` with the filename from your run. An entry marked `succeeded`
means the flow passed its current evaluation. Reading historical fixtures is covered in
[reading results](../results/reading-results.md).

## 4. Understand cleanup

This example creates traffic pods in `tft-docs`. TFT labels its resources with
`tft-tests` and removes them after the run. It deletes the namespace if it created it;
an existing namespace remains. TFT also changes pod-security labels on reused
namespaces, so use a dedicated test namespace.

```bash
kubectl --kubeconfig "$TFT_KUBECONFIG" get namespace tft-docs
```
 `NotFound` is expected when TFT created and then deleted the namespace. To inspect
resources after execution, repeat the run with `--no-cleanup` placed **after** the
positional configuration:

```bash
./tft.py --check quickstart.yaml --no-cleanup
kubectl --kubeconfig "$TFT_KUBECONFIG" -n tft-docs get pods,services
```
 A bare `--no-cleanup` skips final cleanup; it does not skip cleanup between cases.
`--no-cleanup 300` instead delays final cleanup by five minutes. See [running
tests](running-tests.md) for cleanup scope and retained resources.

## Next steps

- [Pick a smoke suite](../configuring/test-cases.md#starter-suites)
- [Choose a traffic tool](../configuring/test-types.md)
- [Set pass/fail thresholds](../results/evaluation.md)
