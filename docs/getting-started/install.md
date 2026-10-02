<!--
SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
SPDX-License-Identifier: Apache-2.0
-->

# Install TFT

Use this page when you need to prepare the machine that runs TFT.

## Python environment

Install `kubectl`, Git, and Python 3.11 on the runner. The recommended runtime is Python
3.11. Work from the repository root so scripts and examples resolve as shown in this
guide.

```bash
git clone https://github.com/ovn-kubernetes/kubernetes-traffic-flow-tests.git
cd kubernetes-traffic-flow-tests
python3.11 -m venv tft-venv
source tft-venv/bin/activate
python -m pip install -r requirements.txt
./tft.py --help
```
 `requirements.txt` downloads a pinned `ktoolbox` wheel from GitHub's raw-content host;
it does not currently install that dependency using a Git clone. Network access to the
Python package index and GitHub is needed, or an administrator must provide the packages
through an approved mirror.

Pass your kubeconfig explicitly when running TFT. Having `kubectl` configured through
its usual defaults does not configure TFT automatically.

## Test images

The runner starts server, client, and plugin pods using
`ghcr.io/ovn-kubernetes/kubernetes-traffic-flow-tests:latest`. The container holds tools
such as iperf3, netperf, curl, and the simple TCP script. Cluster nodes must be able to
pull it. This image supplies the workloads; the Python virtual environment runs the
orchestration on your machine.

The RDMA build target adds perftest and rdma-core. For `ib-*` connections TFT uses
`TFT_RDMA_TEST_IMAGE`, or derives the image from `TFT_TEST_IMAGE` by adding `-rdma`
before the tag: `registry/image:tag` becomes `registry/image-rdma:tag`. With the default
base image this gives
`ghcr.io/ovn-kubernetes/kubernetes-traffic-flow-tests-rdma:latest`. Set the RDMA image
explicitly if your registry uses a different naming convention.

```bash
export TFT_TEST_IMAGE=registry.example.com/team/tft:tested
export TFT_RDMA_TEST_IMAGE=registry.example.com/team/tft-rdma:tested
export TFT_IMAGE_PULL_POLICY=IfNotPresent
```
 Pull policy defaults to `IfNotPresent`, or `Always` when `TFT_TEST_IMAGE` is set. Valid
policies are `IfNotPresent`, `Always`, and `Never`. For a disconnected cluster, mirror
the images and arrange image-pull credentials on the cluster; setting the image variable
alone does not provide credentials.

## Build images

On a Linux machine with Buildah and registry credentials:

```bash
./scripts/build-container.sh registry.example.com/team/tft:tested
export TFT_TEST_IMAGE=registry.example.com/team/tft:tested
```
 The script builds and **pushes** both the base image and `tft-rdma:tested` for amd64
and arm64. It requires registry write access; it is not a local-only build.

## Other container tools

For an external iperf3 server on a host reachable from the cluster:

```bash
podman run --rm -p 5201:5201 ghcr.io/ovn-kubernetes/kubernetes-traffic-flow-tests:latest iperf3 -s -p 5201
```
 The image includes magic-wormhole for file transfer. Run the following on the sending
and receiving machines, replacing `FILE` and `CODE`:

```bash
podman run --rm -ti -v .:/pwd:Z -w /pwd ghcr.io/ovn-kubernetes/kubernetes-traffic-flow-tests:latest wormhole send FILE
podman run --rm -ti -v .:/pwd:Z -w /pwd ghcr.io/ovn-kubernetes/kubernetes-traffic-flow-tests:latest wormhole receive CODE
```
 Collect host interface information with the bundled `ktoolbox-netdev` tool:

```bash
podman run --rm --privileged --network=host ghcr.io/ovn-kubernetes/kubernetes-traffic-flow-tests:latest ktoolbox-netdev
```

## Next steps

- [Run your first test](quickstart.md)
- [Image variables](../configuring/environment-variables.md)
- [Developer setup](../../CONTRIBUTING.md#developer-environment)
