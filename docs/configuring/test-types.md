<!--
SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
SPDX-License-Identifier: Apache-2.0
-->

# Choose a traffic tool

Use this page when you need to decide what to measure and which arguments to use.

A connection's `type` chooses the traffic tool; its case IDs choose the network path.
Hyphenated lower-case values below are accepted alongside enum names. All types have
handlers, but endpoint hardware and transport must support the chosen path. Only iperf
TCP currently runs the `reverse` subtest.

| `type` | Measures / result unit | Main default client arguments | `args` accepted | Image | Choose it for |
| --- | --- | --- | --- | --- | --- |
| `iperf-tcp` | TCP TX/RX throughput, Gbit/s | `-c TARGET -p PORT --json -t DURATION --connect-timeout DURATION*1500`; reverse adds `-R` | Server and client | Base | Throughput and supported allow/deny checks. |
| `iperf-udp` | UDP rate, jitter in ms, packet loss/count/percent | TCP-style setup plus `-u -b 25G` | Server and client | Base | Offered-load/loss checks; choose a sensible bandwidth. |
| `http` | Request and body validation, no bitrate | `curl --fail -s --connect-timeout DURATION*1.5 http://TARGET:PORT/data` | No | Base | Basic connectivity and supported allow/deny checks. |
| `netperf-tcp-stream` | TCP throughput; raw Mbit/s, normalized TX Gbit/s | `-H TARGET -p PORT -t TCP_STREAM -l DURATION` | No | Base | Netperf stream comparisons. |
| `netperf-tcp-rr` | Request/response transactions per second | `-H TARGET -p PORT -t TCP_RR -l DURATION` | No | Base | Transaction rate rather than bulk throughput. |
| `ib-write-bw` | RDMA write bandwidth, Gbit/s | `TARGET --port PORT --report_gbits --duration DURATION` | Server and client | RDMA | RDMA write path on hardware with usable RDMA devices. |
| `ib-read-bw` | RDMA read bandwidth, Gbit/s | Same as write using `ib_read_bw` | Server and client | RDMA | RDMA read path. |
| `ib-send-bw` | RDMA send bandwidth, Gbit/s; raw message rate in Mpps | `TARGET --port PORT --report_gbits`; iterations, no `--duration` | Server and client | RDMA | RDMA send path; set `-n` to control iterations. |
| `simple` | TCP script success, no bitrate | `--addr TARGET --port PORT --duration DURATION` | Server and client | Base | Interactive debugging and custom execution scripts. |

**Netperf RR:** the current handler puts transactions/s divided by 1000 in
`bitrate_gbps.tx`, and the printer labels this as Gbps. That numeric value is
**thousands of transactions/s**, not network bandwidth. Read the raw `Transaction Rate
Per Second` field. A TX threshold of `5` for this type means 5000 transactions/s. RX is
unavailable. TCP stream also only populates TX.

HTTP performs a request rather than a timed throughput measurement; for deny cases it
can retry during the configured duration. With `TFT_EXTERNAL_URL`, HTTP tests the
configured response substring instead of the bundled `/data` response. See [external
traffic](scenarios/external-traffic.md).

RDMA tool `args` are supported in current source, in addition to iperf and simple.
Access to `/dev/infiniband`, a suitable device plugin or site pod configuration,
routing, and NIC support must exist independently of the image. The current RDMA parser
reads the first numeric data row, so avoid options that change the output layout without
checking the result. A successful control connection through a Service does not prove
that RDMA data traverses that Service.

## Tool arguments

Set `args` on a server/client endpoint, not on the connection. Strings use shell word
splitting; lists supply each argument separately. Arguments are appended to generated
commands, so verify the resulting `flow_test.command` when using options that overlap
with generated settings.

```yaml
client:
  - name: worker-2
    args: ["-b", "10G", "--parallel", "4"]
```
 For iperf UDP the default `-b 25G` is added **only when client `args` is absent or
empty**. If you supply other client arguments, include an explicit `-b` to keep the
desired offered load. Server arguments are appended to `iperf3 -s -p PORT`; ordinary
per-run servers add `--one-off --json`.

Use iperf TCP or HTTP for expected-block scenarios. Netperf, RDMA, and simple do not
implement the same handler-level deny checks. The evaluator recognizes non-HTTP
expected-block results by zero or unavailable bitrate, which alone cannot distinguish an
intended deny from another tool failure.

## Debug with simple

`simple` runs
[scripts/simple-tcp-server-client.py](../../scripts/simple-tcp-server-client.py). Its
`args` can include `--exec` to download and run an alternative script; consult the
script's `--help` for `--exec-args`, `--exec-arg`, and TLS options. Use scripts you
control, since they execute inside the traffic pod.

[scripts/simple-exec.sh](../../scripts/simple-exec.sh) calls the bundled TCP script and
hangs after a failure so you can enter the pod and inspect it. Its first non-empty
argument can point to an alternative TCP script, allowing iteration without rebuilding
the container. For example, set these endpoint arguments with your own hosted copy:

```yaml
server:
  - name: worker-1
    args: "--num-clients 0 --exec https://example.com/tft/simple-exec.sh"
client:
  - name: worker-2
    args: "--exec https://example.com/tft/simple-exec.sh"
```
 The URLs are placeholders to replace. Combine with `pre_provision: true` and
`--no-cleanup` when retaining traffic pods for investigation.

## Next steps

- [Add measurements and validation](plugins.md)
- [RDMA scenario](scenarios/rdma.md)
- [Read tool metrics](../results/reading-results.md)
