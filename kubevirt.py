# SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
# SPDX-License-Identifier: Apache-2.0

import base64
import ipaddress
import json
import logging
import re
import shlex
import threading
import time
from collections.abc import Iterable
from typing import Any
from typing import Optional

from ktoolbox import host
from ktoolbox.k8sClient import K8sClient

logger = logging.getLogger("tft." + __name__)

VMI_RESOURCE = "virtualmachineinstance.kubevirt.io"
VMI_LABEL = "tft-vmi"
DATAVOLUME_RESOURCE = "datavolume.cdi.kubevirt.io"
BASE_DISK_NAME = "tft-vmi-base"
_base_disk_lock = threading.Lock()


def _duration_seconds(value: str) -> float:
    units = {
        "h": 3600,
        "m": 60,
        "s": 1,
        "ms": 1e-3,
        "us": 1e-6,
        "µs": 1e-6,
        "μs": 1e-6,
        "ns": 1e-9,
    }
    parts = list(re.finditer(r"(\d+(?:\.\d*)?|\.\d+)(h|ms|m|ns|us|µs|μs|s)", value))
    if value == "0":
        return 0
    if not parts or "".join(part[0] for part in parts) != value:
        raise ValueError(f"Invalid provisioning timeout {value!r}")
    return sum(float(part[1]) * units[part[2]] for part in parts)


class VmiEndpoint:
    def __init__(self, client: K8sClient, namespace: str, name: str) -> None:
        self.client = client
        self.namespace = namespace
        self.name = name

    def get(self) -> dict[str, Any]:
        vmi = self.client.oc_get(
            f"{VMI_RESOURCE}/{self.name}", namespace=self.namespace
        )
        if vmi is None:
            raise RuntimeError(f"Cannot read VMI {self.namespace}/{self.name}")
        return vmi

    @property
    def disk_name(self) -> str:
        return f"{self.name}-root"

    def _ensure_data_volume(
        self, name: str, manifest: str, *, namespace: Optional[str] = None
    ) -> None:
        namespace = self.namespace if namespace is None else namespace
        existing = self.client.oc_get(
            f"{DATAVOLUME_RESOURCE}/{name}", namespace=namespace, may_fail=True
        )
        if existing is not None:
            if existing.get("metadata", {}).get("labels", {}).get(VMI_LABEL) != "true":
                raise RuntimeError(f"Refusing to reuse non-TFT DataVolume {name}")
            return
        if (
            self.client.oc_get(f"pvc/{name}", namespace=namespace, may_fail=True)
            is not None
        ):
            raise RuntimeError(f"Refusing to adopt existing PVC {namespace}/{name}")
        result = self.client.oc(["apply", "-f", manifest], namespace=namespace)
        if not result.success:
            raise RuntimeError(f"Failed to create DataVolume {name}: {result.err}")

    def _wait_disk(
        self,
        resource: str,
        phase: str,
        timeout: str,
        *,
        namespace: Optional[str] = None,
    ) -> None:
        namespace = self.namespace if namespace is None else namespace
        result = self.client.oc(
            [
                "wait",
                resource,
                f"--for=jsonpath={{.status.phase}}={phase}",
                f"--timeout={timeout}",
            ],
            namespace=namespace,
        )
        if not result.success:
            self.client.oc(["describe", resource], namespace=namespace, may_fail=True)
            raise RuntimeError(
                f"Disk {namespace}/{resource} did not reach {phase}: {result.err}"
            )

    def setup(
        self,
        manifest: str,
        timeout: str,
        *,
        base_manifest: str,
        disk_manifest: str,
        base_namespace: Optional[str] = None,
        ready_commands: Iterable[str] = (),
    ) -> None:
        existing = self.client.oc_get(
            f"{VMI_RESOURCE}/{self.name}", namespace=self.namespace, may_fail=True
        )
        if existing is None:
            with _base_disk_lock:
                self._ensure_data_volume(
                    BASE_DISK_NAME, base_manifest, namespace=base_namespace
                )
                self._wait_disk(
                    f"{DATAVOLUME_RESOURCE}/{BASE_DISK_NAME}",
                    "Succeeded",
                    timeout,
                    namespace=base_namespace,
                )
            self._ensure_data_volume(self.disk_name, disk_manifest)
            result = self.client.oc(["apply", "-f", manifest], namespace=self.namespace)
            if not result.success:
                raise RuntimeError(f"Failed to create VMI {self.name}: {result.err}")
        elif existing.get("metadata", {}).get("labels", {}).get(VMI_LABEL) != "true":
            raise RuntimeError(f"Refusing to reuse non-TFT VMI {self.name}")
        # Creating the VMI lets KubeVirt schedule storage with WaitForFirstConsumer.
        self._wait_disk(f"{DATAVOLUME_RESOURCE}/{self.disk_name}", "Succeeded", timeout)
        for condition in ("Ready", "AgentConnected"):
            result = self.client.oc(
                [
                    "wait",
                    f"{VMI_RESOURCE}/{self.name}",
                    f"--for=condition={condition}",
                    f"--timeout={timeout}",
                ],
                namespace=self.namespace,
            )
            if not result.success:
                self.client.oc(
                    ["describe", f"{VMI_RESOURCE}/{self.name}"],
                    namespace=self.namespace,
                    may_fail=True,
                )
                self._log_boot_output()
                raise RuntimeError(
                    f"VMI {self.name} did not reach {condition}: {result.err}"
                )
        checks = ["test -f /var/lib/cloud/instance/boot-finished"]
        checks.extend(
            f"command -v {shlex.quote(command)} >/dev/null"
            for command in ready_commands
        )
        deadline = time.monotonic() + _duration_seconds(timeout)
        while True:
            result = self.run(" && ".join(checks), timeout=10, may_fail=True)
            if result.success:
                break
            if time.monotonic() >= deadline:
                self._log_boot_output()
                raise RuntimeError(
                    f"Guest setup did not finish for VMI {self.namespace}/{self.name}: {result.err}"
                )
            time.sleep(5)

    def _log_boot_output(self) -> None:
        try:
            uid = self.get()["metadata"]["uid"]
            launcher = self.launcher()
            result = self.client.oc(
                [
                    "--request-timeout=30s",
                    "exec",
                    launcher,
                    "-c",
                    "compute",
                    "--",
                    "tail",
                    "-n",
                    "200",
                    f"/var/run/kubevirt-private/{uid}/virt-serial0-log",
                ],
                namespace=self.namespace,
                may_fail=True,
            )
            logger.error(
                "VMI %s/%s serial boot output:\n%s%s",
                self.namespace,
                self.name,
                result.out,
                result.err,
            )
        except (RuntimeError, ValueError, KeyError, TypeError) as exc:
            logger.warning(
                "Cannot read VMI %s/%s boot output: %s", self.namespace, self.name, exc
            )

    def launcher(self) -> str:
        vmi = self.get()
        result = self.client.oc(
            [
                "get",
                "pods",
                "-l",
                f"kubevirt.io/created-by={vmi['metadata']['uid']}",
                "-o",
                "json",
            ],
            namespace=self.namespace,
        )
        if not result.success:
            raise RuntimeError(
                f"Cannot list launcher Pods for VMI {self.namespace}/{self.name}: "
                f"{result.err}"
            )
        pods = [
            pod["metadata"]["name"]
            for pod in json.loads(result.out)["items"]
            if any(
                owner.get("uid") == vmi["metadata"]["uid"]
                for owner in pod["metadata"].get("ownerReferences", [])
            )
        ]
        if len(pods) != 1:
            raise RuntimeError(
                f"Expected one launcher Pod owned by VMI {self.namespace}/{self.name}, "
                f"got {pods}"
            )
        return str(pods[0])

    def _agent(
        self, launcher: str, command: str, arguments: Optional[dict[str, Any]] = None
    ) -> dict[str, Any]:
        request: dict[str, Any] = {"execute": command}
        if arguments is not None:
            request["arguments"] = arguments
        result = self.client.oc(
            [
                "--request-timeout=30s",
                "exec",
                launcher,
                "-c",
                "compute",
                "--",
                "virsh",
                "qemu-agent-command",
                f"{self.namespace}_{self.name}",
                json.dumps(request),
                "--timeout",
                "10",
            ],
            namespace=self.namespace,
            may_fail=True,
        )
        if not result.success:
            raise RuntimeError(f"Guest agent command {command} failed: {result.err}")
        response = json.loads(result.out)
        if not isinstance(response, dict):
            raise RuntimeError(f"Invalid guest agent response to {command}")
        if "error" in response:
            raise RuntimeError(f"Guest agent command {command}: {response['error']}")
        value = response.get("return")
        if not isinstance(value, dict):
            raise RuntimeError(f"Invalid guest agent response to {command}")
        return value

    def run(
        self,
        command: str | Iterable[str],
        *,
        timeout: float,
        may_fail: bool = False,
        die_on_error: bool = False,
    ) -> host.Result:
        try:
            launcher = self.launcher()
            argv = (
                ["/bin/sh", "-c", command]
                if isinstance(command, str)
                else list(command)
            )
            started = self._agent(
                launcher,
                "guest-exec",
                {
                    "path": "/usr/local/libexec/tft-guest-exec",
                    "arg": [
                        "/usr/bin/timeout",
                        "--kill-after=5",
                        str(int(timeout)),
                        *argv,
                    ],
                    "capture-output": True,
                },
            )
            pid = started["pid"]
            deadline = time.monotonic() + timeout + 10
            while True:
                status = self._agent(launcher, "guest-exec-status", {"pid": pid})
                if status.get("exited"):
                    if status.get("out-truncated") or status.get("err-truncated"):
                        raise RuntimeError("Guest command output was truncated by QEMU")
                    returncode = status.get("exitcode")
                    if returncode is None:
                        returncode = int(status["signal"])
                    result = host.Result(
                        base64.b64decode(
                            status.get("out-data", ""), validate=True
                        ).decode("utf-8", "replace"),
                        base64.b64decode(
                            status.get("err-data", ""), validate=True
                        ).decode("utf-8", "replace"),
                        int(returncode),
                    )
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError(f"Guest command timed out after {timeout}s")
                time.sleep(0.2)
        except (RuntimeError, ValueError, KeyError, TypeError) as exc:
            result = host.Result(
                "", f"VMI {self.namespace}/{self.name}: {exc}", 1, cancelled=True
            )
        if not result.success:
            logger.log(logging.DEBUG if may_fail else logging.ERROR, result.debug_msg())
            if die_on_error:
                raise RuntimeError(result.debug_msg())
        return result

    def ip(self, *, timeout: float = 30) -> str:
        deadline = time.monotonic() + timeout
        while True:
            address = self._ip()
            if address is not None:
                return address
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"VMI {self.name} has no IPv4 address for the pod network"
                )
            time.sleep(1)

    def _ip(self) -> Optional[str]:
        vmi = self.get()
        names = {
            network["name"]
            for network in vmi["spec"].get("networks", [])
            if "pod" in network
        }
        for interface in vmi.get("status", {}).get("interfaces", []):
            if interface.get("name") not in names:
                continue
            for address in [
                interface.get("ipAddress", ""),
                *interface.get("ipAddresses", []),
            ]:
                if not address:
                    continue
                ip = ipaddress.ip_address(address)
                if ip.version == 4 and not ip.is_loopback and not ip.is_link_local:
                    return str(ip)
        return None
