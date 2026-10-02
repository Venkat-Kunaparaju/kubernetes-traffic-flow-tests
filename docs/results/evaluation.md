<!--
SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
SPDX-License-Identifier: Apache-2.0
-->

# Evaluate pass and fail

Use this page when you need thresholds and reliable CI exit status.

TFT evaluates every completed run, using either the supplied threshold YAML or an empty
configuration. Evaluation combines traffic/tool outcome, expected blocking, directional
thresholds, and independent plugin results. Missing thresholds disable the corresponding
**rate check**; they do not make an ordinary failed flow or failed plugin pass.

## Evaluate during the run

```bash
./tft.py --check --kubeconfig /path/tenant.yaml quickstart.yaml examples/eval/smoke.yaml
```
 The optional second positional argument is the evaluation config. Omit it or pass `''`
for no throughput thresholds. Empty YAML and `{}` are also valid. The test JSON already
contains each flow's `eval_result`.

## Threshold format

```yaml
IPERF_TCP:
  - id: POD_TO_POD_DIFF_NODE
    Normal:
      threshold: 1
    Reverse:
      threshold_rx: 0.8
      threshold_tx: 0.9
```
 Top-level keys are test types; each list item selects a case by ID or enum name, then
gives `Normal` / `Reverse` settings. `threshold` applies to both RX and TX, or use
`threshold_rx` and/or `threshold_tx`. Do not combine `threshold` with directional
thresholds in the same direction mapping. Missing directions or null/empty mappings
apply no thresholds.

Thresholds are lower bounds in Gbit/s for throughput types. Equality passes: with a
threshold of 1, a measured rate of 1 passes and 0.9 fails. RX is checked before TX.
Netperf RR is the exception: current TX is transactions/s divided by 1000, so a TX
threshold of 1 means 1000 transactions/s. HTTP and simple have no measured rate, and
current code treats unavailable bitrate as passing a rate check even when a threshold is
set. Do not use their bitrate thresholds to establish performance.

The evaluation key consists of **test type + case ID + normal/reverse**. It does not
distinguish connection name, nodes, instance, target-access mode, resource budget, or
RuntimeClass. Every such variant shares the corresponding threshold. Keep results from
different environments in separate baselines if they need different limits.

## Passing and failing examples

Suppose a non-reverse case 2 iperf TCP flow succeeds and measures RX 2.4 and TX
2.5 Gbit/s. `Normal: {threshold: 1}` passes both directions. Changing that to
`Normal: {threshold: 3}` fails the RX check and records a message saying the
run succeeded but the rate is below the RX threshold. These numbers illustrate
the decision; they are not measurements from a cluster run.

A failed traffic tool normally fails evaluation regardless of thresholds. For non-HTTP
expected-block cases, evaluation instead checks for nonzero measured RX/TX: positive
throughput fails, zero or unavailable throughput passes. HTTP expected-block decisions
come from the HTTP handler's connection check. Pair deny tests with working allow tests
so a broken path is not mistaken for correct policy enforcement.

Every plugin's recorded validation is also checked. A failed plugin makes the aggregate
entry fail even if the flow passes its rate thresholds. Evaluation YAML does not set
CPU, power, ping, or representor packet limits.

## Re-evaluate saved results

```bash
./evaluator.py examples/eval/smoke.yaml ft-logs/run-000.json /tmp/tft-rechecked.json
./print_results.py --no-color /tmp/tft-rechecked.json
```
 `evaluator.py` takes exactly three positional arguments: evaluation config, input JSON,
and output JSON. It writes the same current result format and logs updated pass counts.
`-v` / `--verbose` enables debug logging; `-h` shows help. It changes the evaluation,
not the recorded measurements, and does not rerun traffic or plugins.

To remove earlier rate thresholds before generating a baseline:

```bash
./evaluator.py '' ft-logs/run-000.json /tmp/tft-no-thresholds.json
```

## Summary and exit codes

The runner and evaluator log `RESULT: Success = True/False`, flow pass counts, and
plugin pass counts. Plugins can produce multiple endpoint results per flow.

| Command | Meaning of exit status |
| --- | --- |
| `tft.py CONFIG [EVAL]` | Ordinary completed run returns `0` even if recorded results fail; configuration/runtime errors can fail. |
| `tft.py --check CONFIG [EVAL]` | `0` when all recorded flows/plugins pass, `1` when validation fails; serious errors can use other nonzero statuses. |
| `print_results.py RESULT...` | `0` when all files pass, `1` when any aggregate entry fails; parse errors fail separately. |
| `evaluator.py EVAL INPUT OUTPUT` | Writes/logs updated evaluation; threshold failure by itself does **not** set a failing exit status. Follow with `print_results.py` for CI enforcement. |

Use `--check` on the runner or the printer after re-evaluation when pass/fail must
control a pipeline. Reading an evaluator's successful process status alone does not
prove the flows passed.

## Next steps

- [Generate thresholds from known-good runs](baselines.md)
- [Read results](reading-results.md)
- [Example evaluation files](../../examples/README.md)
