import os
import subprocess
import tempfile
from typing import Any
from typing import Optional

import yaml

from ktoolbox import common

from tftbase import TestCaseType

logger = common.ExtendedLogger("tft." + __name__)


def _cases_with(substring: str) -> list[str]:
    return [tc.name for tc in TestCaseType if substring in tc.name]


_SECOND_IFACE_TEST_CASES = _cases_with("2ND_INTERFACE")
_MNP_TEST_CASES = _cases_with("MULTI_NETWORK_POLICY")
_LB_TEST_CASES = _cases_with("LOAD_BALANCER")
_UDN_TEST_CASES = _cases_with("UDN")
_ANP_TEST_CASES = _cases_with("ANP")
_EXTERNAL_TEST_CASES = _cases_with("EXTERNAL")
_NGN_TEST_CASES = ["NGN"]

_SPECIAL_TEST_CASES = set(
    _SECOND_IFACE_TEST_CASES
    + _MNP_TEST_CASES
    + _LB_TEST_CASES
    + _UDN_TEST_CASES
    + _ANP_TEST_CASES
    + _EXTERNAL_TEST_CASES
    + _NGN_TEST_CASES
)

_BASE_TEST_CASES = [
    tc.name for tc in TestCaseType if tc.name not in _SPECIAL_TEST_CASES
]


class _Kubectl:
    def __init__(self, kubeconfig: Optional[str] = None) -> None:
        self._base = ["kubectl", "--kubeconfig", kubeconfig]

    def run(self, *args: str, may_fail: bool = False) -> tuple[str, bool]:
        cmd = self._base + list(args)
        r = subprocess.run(cmd, capture_output=True, text=True)
        ok = r.returncode == 0
        if not ok and not may_fail:
            raise RuntimeError(f"kubectl {' '.join(args)} failed: {r.stderr.strip()}")
        return r.stdout.strip(), ok

    def jsonpath(
        self,
        resource: str,
        namespace: str,
        path: str,
        *,
        may_fail: bool = True,
    ) -> str:
        out, _ = self.run(
            "get",
            resource,
            "-n",
            namespace,
            f"-o=jsonpath={path}",
            may_fail=may_fail,
        )
        return out.strip().strip("'\"")

    def ns_exists(self, namespace: str) -> bool:
        _, ok = self.run("get", "ns", namespace, may_fail=True)
        return ok


class NGN:
    """Config generator for NGN clusters."""

    # Default secondary NAD for CS/SS hosts
    _SECONDARY_NAD = "default/ovn-stream-vf"

    _SERVER_PATTERN = ""
    _CLIENT_PATTERN = ""
    _NAMESPACE = "ft"
    _DURATION = "5"
    _CONN_TYPES = ("iperf-tcp", "http")
    _HTTP_POD_PORT = 8080
    _HTTP_HOST_PORT = 9898
    _IPERF_POD_PORT = 5201
    _IPERF_HOST_PORT = 5202
    _SRIOV_RESOURCE = "nvidia.com/asap2_vf"
    _USE_VFS = "true"
    _VARS = "true"
    _UDN_MODE = "false"
    _PERF_MIN_TPUT: float = 15.0

    _SKIP_TESTS = []

    # Base test cases for every NGN zone
    _TEST_CASES = _BASE_TEST_CASES + _SECOND_IFACE_TEST_CASES + _MNP_TEST_CASES

    @staticmethod
    def _pick_node(kubectl: _Kubectl, pattern: str, exclude: str = "") -> Optional[str]:
        out, _ = kubectl.run(
            "get", "nodes", "--no-headers", "--show-labels", may_fail=True
        )
        for line in out.splitlines():
            parts = line.split()
            if not parts:
                continue
            node_name = parts[0]
            status = parts[1] if len(parts) > 1 else ""
            if "Ready" not in status:
                continue
            if exclude and node_name == exclude:
                continue
            if not pattern or pattern in line:
                return node_name
        return None

    @staticmethod
    def _node_has_resource(kubectl: _Kubectl, node: str, resource: str) -> bool:
        out, ok = kubectl.run(
            "get",
            "node",
            node,
            f"-o=jsonpath={{.status.capacity.{resource}}}",
            may_fail=True,
        )
        if not ok:
            return False
        return out.strip() not in ("", "null")

    @staticmethod
    def _multi_vtep_eligible(
        kubectl: _Kubectl, server_node: str, host_type: str
    ) -> bool:
        if host_type not in ("CS", "SS"):
            return False

        # Check 1: skip if OVS-DOCA is enabled on the server node.
        ovnkube_out, _ = kubectl.run(
            "get",
            "pods",
            "-n",
            "ovn-kubernetes",
            "-l",
            "app=ovnkube-node",
            f"--field-selector=spec.nodeName={server_node}",
            "--no-headers",
            "-o",
            "custom-columns=:metadata.name",
            may_fail=True,
        )
        ovnkube_pod = (ovnkube_out.splitlines() or [""])[0].strip()
        if ovnkube_pod:
            dpdk, _ = kubectl.run(
                "exec",
                ovnkube_pod,
                "-n",
                "ovn-kubernetes",
                "--",
                "ovs-vsctl",
                "get",
                "open",
                ".",
                "dpdk_initialized",
                may_fail=True,
            )
            if dpdk.strip('"') == "true":
                logger.info("DOCA (OVS-DPDK) detected on server — skipping multi-vtep.")
                return False

        # Check 2: skip if server is in DPU mode (dpus annotation present).
        dpus = kubectl.jsonpath(
            f"node/{server_node}",
            "default",
            r"{.metadata.annotations.ngn2\.nvidia\.com/dpus}",
        )
        if dpus:
            logger.info("Server in DPU mode — skipping multi-vtep.")
            return False

        # Check 3: skip if SS node has only one NIC.
        if host_type == "SS":
            node_yaml, _ = kubectl.run(
                "get", "node", server_node, "-o", "yaml", may_fail=True
            )
            nics = {
                line.strip()
                for line in node_yaml.splitlines()
                if "nvidia.com/" in line and "_nic" in line
            }
            nic_count = len(nics) + 1
            if nic_count <= 1:
                logger.info(
                    f"SS node has only {nic_count} NIC(s) — skipping multi-vtep."
                )
                return False
            logger.info(f"SS node has {nic_count} NICs — multi-vtep eligible.")

        return True

    @staticmethod
    def _generate_eval_config(test_cases: list[str], threshold: float) -> str:
        # Exclude host-network test cases (matching SKIP_HOST_POD_PERF_TEST_EVAL=true
        # Only evaluate the Normal (non-reverse)
        # direction, matching the flip=false check in test.sh.
        entries = [
            {"id": tc, "Normal": {"threshold": threshold}}
            for tc in test_cases
            if "HOST" not in tc
        ]
        fd, path = tempfile.mkstemp(suffix=".yaml", prefix="tft-eval-config-")
        try:
            with os.fdopen(fd, "w") as f:
                yaml.dump({"IPERF_TCP": entries}, f, default_flow_style=False)
        except Exception:
            os.close(fd)
            raise
        logger.info(
            f"Generated eval-config with threshold={threshold} Gbps "
            f"for {len(entries)} test cases (host excluded): {path}"
        )
        return path

    @staticmethod
    def auto_generate_config() -> dict[str, Any]:

        kubeconfig = os.environ.get("KUBECONFIG") or os.path.expanduser(
            "~/.kube/config"
        )
        kubectl = _Kubectl(kubeconfig)

        # Require an NGN cluster.
        zone_info = kubectl.jsonpath(
            "cm/zone-info",
            "flux-system",
            r"{.data.values\.yaml}",
        )
        if not zone_info:
            raise RuntimeError(
                "NGN cluster not detected (flux-system/zone-info ConfigMap missing).\n"
                "Pass a config file explicitly: ./tft.py <config.yaml>"
            )
        logger.info("NGN cluster detected.")

        # Zone type — metro zones need different node patterns.
        is_mrz = False
        for line in zone_info.splitlines():
            if line.startswith("hardware_configuration:"):
                zone_type = line.split(":", 1)[1].strip().strip('"')
                logger.info(f"Zone type: {zone_type}")
                if zone_type == "metro":
                    is_mrz = True
                    logger.info("MRZ (metro) zone detected.")
                break

        server_pattern = NGN._SERVER_PATTERN
        client_pattern = NGN._CLIENT_PATTERN

        if is_mrz:
            mrz_pattern = "hostrole=nvmesh"
            if server_pattern in ("CS", "SS", "GS"):
                server_pattern = mrz_pattern
                logger.info(f"MRZ: overriding server pattern to '{mrz_pattern}'")
            if client_pattern in ("CS", "SS", "GS"):
                client_pattern = mrz_pattern
                logger.info(f"MRZ: overriding client pattern to '{mrz_pattern}'")

        # Node auto-detection.
        server_node = NGN._pick_node(kubectl, server_pattern)
        if not server_node:
            raise RuntimeError(
                f"Could not auto-detect server node with pattern '{server_pattern}'."
            )
        logger.info(f"Auto-detected server node: {server_node}")

        client_node = NGN._pick_node(kubectl, client_pattern, exclude=server_node)
        if not client_node:
            raise RuntimeError(
                f"Could not auto-detect client node with pattern '{client_pattern}'."
            )
        logger.info(f"Auto-detected client node: {client_node}")

        use_vfs = NGN._USE_VFS.lower() == "true"
        udn_mode = NGN._UDN_MODE.lower() == "true"

        # SR-IOV resource availability check.
        sriov_resource: Optional[str] = None
        if NGN._SRIOV_RESOURCE and use_vfs:
            server_ok = NGN._node_has_resource(
                kubectl, server_node, NGN._SRIOV_RESOURCE
            )
            client_ok = NGN._node_has_resource(
                kubectl, client_node, NGN._SRIOV_RESOURCE
            )
            if server_ok and client_ok:
                sriov_resource = NGN._SRIOV_RESOURCE
                logger.info(
                    f"SR-IOV resource '{NGN._SRIOV_RESOURCE}' available on both nodes."
                )
            else:
                logger.info(
                    f"SR-IOV resource '{NGN._SRIOV_RESOURCE}' not available on "
                    f"server ({server_ok}) and/or client ({client_ok}) — "
                    f"omitting resource_name from config."
                )

        # Host-type labels.
        host_type = (
            kubectl.jsonpath(
                f"node/{server_node}",
                "default",
                r"{.metadata.labels.ngn2\.nvidia\.com/hosttype}",
            )
            or "GS"
        )
        logger.info(f"Server host type: {host_type}")

        # Feature detection.
        has_metallb = kubectl.ns_exists("metallb-system")
        logger.info(f"MetalLB: {'detected' if has_metallb else 'not detected'}")

        igw_vip = kubectl.jsonpath(
            f"node/{client_node}",
            "default",
            r"{.metadata.labels.ngn2\.nvidia\.com/igw_vip}",
        )
        has_igw = bool(igw_vip)
        logger.info(f"IGW: {'detected' if has_igw else 'not detected'}")

        has_mrc = False
        has_mrz_plugins = False
        if not is_mrz:
            mrc_info = kubectl.jsonpath(
                "cm/mrc-zone-info", "flux-system", r"{.data.values\.yaml}"
            )
            if mrc_info and "proxy_lb_ip:" in mrc_info:
                has_mrc = True
                logger.info("MRC detected.")
            else:
                logger.info("MRC not detected.")

            mrz_info = kubectl.jsonpath(
                "cm/nvmesh-zone-info", "flux-system", r"{.data.values\.yaml}"
            )
            if mrz_info:
                for line in mrz_info.splitlines():
                    if line.startswith("mrZone:") and line.split(":", 1)[1].strip():
                        has_mrz_plugins = True
                        logger.info("MRZ plugins applicable.")
                        break
            if not has_mrz_plugins:
                logger.info("MRZ plugins not applicable.")
        else:
            logger.info("MRZ zone: skipping MRC and MRZ plugin detection.")

        # Default secondary NAD for CS/SS hosts.
        secondary_nad: Optional[str] = None
        if host_type in ("CS", "SS"):
            secondary_nad = NGN._SECONDARY_NAD

        # Resolve _SKIP_TESTS to a set of names (supports str names or int values).
        skip_names = set()
        for entry in NGN._SKIP_TESTS:
            if isinstance(entry, str):
                skip_names.add(entry)
            elif isinstance(entry, int):
                skip_names.add(TestCaseType(entry).name)
        if skip_names:
            logger.info(f"Skipping test cases: {sorted(skip_names)}")

        # Build test cases.
        test_cases: list[str] = list(_BASE_TEST_CASES)

        if secondary_nad is not None:
            logger.info("Including second-interface and MNP test cases.")
            test_cases += _SECOND_IFACE_TEST_CASES
            test_cases += _MNP_TEST_CASES
        else:
            logger.info(
                "No secondary NAD — omitting second-interface and MNP test cases."
            )

        if has_metallb:
            logger.info("Including load-balancer test cases.")
            test_cases += _LB_TEST_CASES

        if udn_mode:
            logger.info("UDN mode enabled — including UDN test cases.")
            test_cases += _UDN_TEST_CASES

        # Multi-vtep plugins
        _MULTI_VTEP_TEST_CASES = [
            "POD_TO_POD_DIFF_NODE",
            "POD_TO_POD_2ND_INTERFACE_DIFF_NODE",
        ]
        multi_vtep_plugins: list[dict[str, Any]] = []
        if NGN._multi_vtep_eligible(kubectl, server_node, host_type):
            logger.info("Adding multi-vtep plugin.")
            multi_vtep_plugins.append(
                {"name": "multi_vtep", "test_cases": _MULTI_VTEP_TEST_CASES}
            )

        # NGN-specific plugins scoped to the NGN test case.
        ngn_plugins: list[dict[str, Any]] = []
        if has_igw:
            ngn_plugins.append({"name": "validate_igw", "test_cases": _NGN_TEST_CASES})
        if has_mrc:
            ngn_plugins.append({"name": "validate_mrc", "test_cases": _NGN_TEST_CASES})
        if has_mrz_plugins:
            ngn_plugins.append(
                {"name": "validate_mrz_kafka", "test_cases": _NGN_TEST_CASES}
            )
            ngn_plugins.append(
                {"name": "validate_mrz_nvmesh", "test_cases": _NGN_TEST_CASES}
            )

        if ngn_plugins:
            test_cases += _NGN_TEST_CASES

        # Apply skip list.
        if skip_names:
            test_cases = [tc for tc in test_cases if tc not in skip_names]

        plugins_cfg: list[dict[str, Any]] = multi_vtep_plugins + ngn_plugins

        def _build_connection(conn_type: str) -> dict[str, Any]:
            if conn_type == "http":
                pod_port = NGN._HTTP_POD_PORT
                host_port = NGN._HTTP_HOST_PORT
            else:
                pod_port = NGN._IPERF_POD_PORT
                host_port = NGN._IPERF_HOST_PORT
            conn: dict[str, Any] = {
                "name": f"Connection_{conn_type}",
                "type": conn_type,
                "reverse": False,
                "server": [
                    {
                        "name": server_node,
                        "pod_port": pod_port,
                        "host_port": host_port,
                        "persistent": True,
                    }
                ],
                "client": [{"name": client_node}],
            }
            # Attach plugins to the HTTP connection if present, otherwise iperf.
            plugin_conn_type = (
                "http" if "http" in NGN._CONN_TYPES else NGN._CONN_TYPES[0]
            )
            if plugins_cfg and conn_type == plugin_conn_type:
                conn["plugins"] = plugins_cfg
            if secondary_nad:
                conn["secondary_network_nad"] = secondary_nad
            if sriov_resource:
                conn["resource_name"] = sriov_resource
            return conn

        connections = [_build_connection(ct) for ct in NGN._CONN_TYPES]

        eval_config_path: Optional[str] = None
        if "iperf-tcp" in NGN._CONN_TYPES:
            eval_config_path = NGN._generate_eval_config(test_cases, NGN._PERF_MIN_TPUT)

        cfg: dict[str, Any] = {
            "tft": [
                {
                    "name": "NGN Config",
                    "namespace": NGN._NAMESPACE,
                    "test_cases": test_cases,
                    "duration": NGN._DURATION,
                    "pre_provision": True,
                    "connections": connections,
                }
            ],
            "kubeconfig": kubeconfig or "",
            "_generated_eval_config": eval_config_path,
        }

        if NGN._VARS.lower() == "true":
            lines = ["", "=== Generated NGN Config ==="]
            tft_entry = cfg["tft"][0]
            lines.append(f"  name:         {tft_entry['name']}")
            lines.append(f"  namespace:    {tft_entry['namespace']}")
            lines.append(f"  duration:     {tft_entry['duration']}")
            lines.append(f"  pre_provision:{tft_entry['pre_provision']}")
            lines.append(f"  kubeconfig:{cfg['kubeconfig'] or '(default)'}")
            lines.append(f"  test_cases ({len(test_cases)}):")
            for tc in test_cases:
                lines.append(f"    - {tc}")
            for conn in connections:
                conn_plugins: list[dict[str, Any]] = conn.get("plugins", [])
                lines.append(f"  connection: {conn['name']} (type={conn['type']})")
                lines.append(f"    server: {conn['server'][0]['name']}")
                lines.append(f"    client: {conn['client'][0]['name']}")
                if secondary_nad:
                    lines.append(f"    secondary_network_nad: {secondary_nad}")
                if sriov_resource:
                    lines.append(f"    resource_name: {sriov_resource}")
                if conn_plugins:
                    lines.append(f"    plugins ({len(conn_plugins)}):")
                    for p in conn_plugins:
                        tc_filter = p.get("test_cases")
                        if tc_filter:
                            lines.append(
                                f"      - {p['name']}  [test_cases: {tc_filter}]"
                            )
                        else:
                            lines.append(f"      - {p['name']}")
                else:
                    lines.append("    plugins: (none)")
            if eval_config_path is not None:
                lines.append(
                    f"  eval_config: {eval_config_path} "
                    f"(threshold={NGN._PERF_MIN_TPUT} Gbps, host excluded, normal only)"
                )
            lines.append("=" * 28)
            logger.info("\n".join(lines))
        return cfg
