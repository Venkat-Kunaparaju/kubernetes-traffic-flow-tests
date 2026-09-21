import base64
import ipaddress
import json
import logging
import time
from collections.abc import Iterable
from typing import Any
from typing import Optional

from ktoolbox import host
from ktoolbox.k8sClient import K8sClient

logger = logging.getLogger("tft." + __name__)

VMI_RESOURCE = "virtualmachineinstance.kubevirt.io"
VMI_LABEL = "tft-vmi"
RESOURCE_ANNOTATION = "k8s.v1.cni.cncf.io/resourceName"


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

    def setup(self, manifest: str, timeout: str) -> bool:
        existing = self.client.oc_get(
            f"{VMI_RESOURCE}/{self.name}", namespace=self.namespace, may_fail=True
        )
        if existing is None:
            result = self.client.oc(["apply", "-f", manifest], namespace=self.namespace)
            if not result.success:
                raise RuntimeError(f"Failed to create VMI {self.name}: {result.err}")
        elif existing.get("metadata", {}).get("labels", {}).get(VMI_LABEL) != "true":
            raise RuntimeError(f"Refusing to reuse non-TFT VMI {self.name}")
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
                raise RuntimeError(
                    f"VMI {self.name} did not reach {condition}: {result.err}"
                )
        self.run(["/bin/true"], timeout=30, die_on_error=True)
        return existing is None

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
            raise RuntimeError(f"Cannot find launcher for {self.name}: {result.err}")
        pods = [
            pod["metadata"]["name"]
            for pod in json.loads(result.out)["items"]
            if not pod["metadata"].get("deletionTimestamp")
            and pod.get("status", {}).get("phase") == "Running"
            and pod.get("spec", {}).get("nodeName") == vmi["status"]["nodeName"]
            and any(
                owner.get("uid") == vmi["metadata"]["uid"]
                for owner in pod["metadata"].get("ownerReferences", [])
            )
        ]
        if len(pods) != 1:
            raise RuntimeError(
                f"Expected one active launcher for {self.name}, got {pods}"
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
                        returncode = 128 + int(status["signal"])
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

    def ip(self, nad: Optional[str] = None, *, timeout: float = 30) -> str:
        deadline = time.monotonic() + timeout
        while True:
            address = self._ip(nad)
            if address is not None:
                return address
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"VMI {self.name} has no IPv4 address for {nad or 'pod network'}"
                )
            time.sleep(1)

    def _ip(self, nad: Optional[str]) -> Optional[str]:
        vmi = self.get()
        names = {
            network["name"]
            for network in vmi["spec"].get("networks", [])
            if (nad is None and "pod" in network)
            or (nad is not None and network.get("multus", {}).get("networkName") == nad)
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
