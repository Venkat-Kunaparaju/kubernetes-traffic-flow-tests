<!--
SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
SPDX-License-Identifier: Apache-2.0
-->

# RDMA traffic

Use this page when you need perftest write, read, or send bandwidth measurements.

## Cluster prerequisites

OVN-Kubernetes is not required by perftest itself. Prepare RDMA NICs and an appropriate
device plugin or site pod configuration that exposes usable RDMA devices to the traffic
containers. An image containing perftest does not grant access to `/dev/infiniband`. Run
`rdma link show` on the relevant Linux hosts; verify device visibility inside equivalent
pods before using this recipe.

Replace `mlx5_0` with the device visible in each endpoint. If your site requires VF
allocation or custom volume/security settings, use the SR-IOV fields and/or manifest
overrides documented in the configuration and environment references. This example only
selects the RDMA tool and device; it cannot configure the hardware or device injection
for you.

Check access and the relevant prerequisites with these read-only commands. Replace
site-specific names and paths before running:

```bash
kubectl get nodes -o wide
kubectl get daemonsets -A
rdma link show
```

## Minimal configuration

Copy [examples/configs/rdma.yaml](../../../examples/configs/rdma.yaml), replace its
placeholders, and run from the repository root with the tenant kubeconfig explicitly
supplied:

```bash
./tft.py --check --kubeconfig /path/tenant.yaml examples/configs/rdma.yaml
```

```yaml
tft:
- name: RDMA traffic
  namespace: tft-docs
  test_cases: 1,2
  duration: 10
  connections:
  - name: traffic
    type: ib-write-bw
    instances: 1
    reverse: false
    server:
    - name: worker-1
      args:
      - -d
      - mlx5_0
    client:
    - name: worker-2
      args:
      - -d
      - mlx5_0
```

## Relevant cases and fields

This example selects `1,2`. See the [catalog](../test-cases.md) for path and placement
and the [configuration reference](../config-reference.md) for field defaults and
precedence.

## Resources and cleanup

Traffic pods using the RDMA test image. TFT does not create device plugins, NIC/VF
configuration, or an RDMA fabric.

Use a dedicated namespace. Existing namespace labels can be changed; see [cleanup
behavior](../../getting-started/running-tests.md#namespaces-and-cleanup).

## Expected results

The tool completes and reports average bandwidth in Gbit/s; TFT stores normalized TX/RX
values and raw perftest text. Confirm the actual device and data path before treating a
control connection as RDMA path validation.

Inspect the recorded JSON with `./print_results.py --no-color RESULT.json`. `--check`
makes recorded flow/plugin failures fail the runner command.

## Variations

Change `type` to `ib-read-bw` or `ib-send-bw`. Write/read use `--duration`; send uses
iterations, so set client `args: ["-d", "mlx5_0", "-n", "5000"]` when you need an
explicit iteration count. None adds a reverse subtest.

`TFT_RDMA_TEST_IMAGE` overrides the RDMA image. Its default is derived from the base
image as `<image>-rdma:<tag>`. TCP/UDP Service or policy behavior does not guarantee
support for an RDMA path; start with direct pod-to-pod cases on a prepared deployment.

## Next steps

- [Configuration reference](../config-reference.md)
- [Environment variables](../environment-variables.md)
- [Reading results](../../results/reading-results.md)
