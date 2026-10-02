<!--
SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
SPDX-License-Identifier: Apache-2.0
-->

# Run and control tests

Use this page when you need command-line options, kubeconfig rules, or cleanup details.

## Command-line reference

```bash
./tft.py [options] CONFIG [EVALUATOR_CONFIG]
./tft.py --check --kubeconfig "$HOME/.kube/config" -o ft-logs/smoke- examples/configs/smoke.yaml examples/eval/smoke.yaml
```

| Argument | Default | Meaning and example |
| --- | --- | --- |
| `CONFIG` | Required | Test configuration YAML, e.g. `quickstart.yaml`. |
| `EVALUATOR_CONFIG` | Empty evaluation | Optional threshold YAML, e.g. `examples/eval/smoke.yaml`; `''` means empty evaluation. |
| `-h`, `--help` | — | Show usage and exit. |
| `-o`, `--output-base` | Unset | Replace `logs`/timestamp naming; `-o ft-logs/smoke-` writes `smoke-000.json`, `smoke-001.json`, etc. |
| `-c`, `--check` | `false` | Return `1` for recorded flow/plugin failures; useful in CI. |
| `--no-check` | Same as default | Return `0` after an ordinary completed run even when some results fail. Startup errors can still fail. |
| `--no-cleanup [SECONDS]` | `0` | Bare flag skips final cleanup; `--no-cleanup 300` delays it by 300 seconds. |
| `--kubeconfig` | Lookup below | Tenant/single-cluster kubeconfig, e.g. `--kubeconfig /path/tenant.yaml`. |
| `--kubeconfig-infra` | Lookup below | DPU infra kubeconfig; set the tenant flag in the same command. |
| `-v`, `--verbosity` | `info` | One of `debug`, `info`, `warning`, `error`, `critical`; e.g. `-v debug`. Overrides `KTOOLBOX_LOGLEVEL`. |

Put a bare `--no-cleanup` after the config arguments, or it can consume the next
argument as its optional integer. There is no case-selection CLI flag: edit `test_cases`
in YAML to choose a subset.

## Kubeconfig lookup

The **pair** of tenant and infra kubeconfigs comes from the first configured source in
this order:

1. CLI `--kubeconfig` / `--kubeconfig-infra`.
2. Environment `TFT_KUBECONFIG` / `TFT_KUBECONFIG_INFRA`.
3. Top-level YAML `kubeconfig` / `kubeconfig_infra`.
4. Built-in file detection.

An infra value requires a tenant value from the same source. A CLI tenant value with no
CLI infra value selects single-cluster mode even if YAML specifies an infra file. Values
from different sources are not combined. CLI/environment relative paths resolve from the
working directory; YAML kubeconfig paths resolve from the configuration file's
directory.

File detection first tries `/root/kubeconfig.nicmodecluster`, then
`/root/kubeconfig.smartniccluster`, then the pair `/root/kubeconfig.tenantcluster` and
`/root/kubeconfig.infracluster`. Finding the tenant file without its infra file is an
error. TFT does not automatically fall back to `~/.kube/config` or the ordinary
`KUBECONFIG` variable.

## Multiple tests and run time

Entries under `tft:` run sequentially in YAML order. Within each entry, TFT runs the
union of selected cases in selection order, selected connections, instances, and
target-access variants. Reverse iperf TCP runs follow each normal run. Instances
currently run sequentially despite a console message referring to simultaneous
connections.

For a timed throughput suite, estimate:

```text
traffic time ≈ sum over connections and cases (
  instances × target-access variants × directions × duration
)
```
 Four cases, two iperf TCP connections, one instance, ten seconds, and reverse enabled
take about `4 × 2 × 1 × 1 × 2 × 10 = 160` seconds of traffic. Add image pulls,
readiness, setup, teardown, retries, and tool startup. Extra Service variants multiply
this: ClusterIP/LoadBalancer have two variants and NodePort three when
`TFT_ENABLE_TARGET_ACCESS_SUBTESTS=true`. HTTP does not normally run for the entire
duration; RDMA send uses iteration-based execution.

For a quick subset use `test_cases: "1"`, `duration: 5`, `instances: 1`, and `reverse:
false`. **Omitted or zero duration means 3600 seconds**, so always set it for short
runs.

## Namespaces and cleanup

Use a dedicated namespace. TFT creates it when needed and sets privileged pod-security
labels even when reusing it. Ordinary final cleanup removes TFT-labelled pods, Services,
NADs, policies, and applicable network resources. TFT deletes base and UDN namespaces
only when it created them. Existing namespaces and their changed labels remain.
Cluster-scoped labelled resources can be cleaned across runs: avoid concurrent suites
sharing TFT's labels or namespace names.

`persistent: true` on a server keeps its server process available for reuse within a
suite; it does not exempt its pod from final cleanup. `pre_provision: true` creates
reusable traffic pods and Services before running cases. Plugins and external Podman
servers still have per-run work. Final cleanup applies to both modes.

`--no-cleanup` controls final cleanup, including newly created infra namespaces.
Per-case cleanup still happens when `pre_provision` is false, and stale TFT resources
are cleaned at startup. To keep traffic pods useful for inspection, combine
`pre_provision: true` with `--no-cleanup`. Afterwards, clean only the resources owned by
this run; deleting a dedicated namespace does not remove cluster-scoped ANP, CUDN,
RouteAdvertisements, or EgressIP resources.

## Common environment controls

Set `TFT_TEST_IMAGE` for a mirrored image, `TFT_IMAGE_PULL_POLICY` for its pull policy,
`TFT_POD_BRINGUP_TIMEOUT` for readiness (default `2m`), and `TFT_LOG_PREAMBLE=false` for
simpler logs. Service access can be varied with `TFT_ENABLE_TARGET_ACCESS_SUBTESTS`. See
the [complete environment reference](../configuring/environment-variables.md) for
defaults and feature-specific settings.

## Next steps

- [Configuration reference](../configuring/config-reference.md)
- [Output files](../results/output-files.md)
- [Evaluation and exit codes](../results/evaluation.md)
