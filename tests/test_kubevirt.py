import base64
import json
import os
from pathlib import Path
import sys
from typing import Any
from unittest import mock

import pytest
import yaml
from ktoolbox import host

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import kubevirt  # noqa: E402
import task  # noqa: E402
import testConfig  # noqa: E402
import testSettings  # noqa: E402
import tftbase  # noqa: E402
import trafficFlowTests  # noqa: E402


def config(**overrides: Any) -> testConfig.TestConfig:
    data: dict[str, Any] = {
        "namespace": "ft",
        "is_vm": True,
        "test_cases": [1, 2],
        "duration": 5,
        "connections": [
            {
                "type": "iperf-tcp",
                "server": [{"name": "worker-1"}],
                "client": [{"name": "worker-2"}],
            }
        ],
    }
    data.update(overrides)
    return testConfig.TestConfig(
        full_config={"tft": [data]}, kubeconfigs=("/test/kubeconfig", None)
    )


def endpoints(
    tc: testConfig.TestConfig, case_index: int = 0, instance: int = 0
) -> tuple[task.ServerTask, task.ClientTask]:
    descriptor = testConfig.ConfigDescriptor(
        tc, tft_idx=0, test_cases_idx=case_index, connections_idx=0
    )
    with mock.patch.object(tc, "validate_selected_nodes_available"):
        settings = testSettings.TestSettings(
            cfg_descr=descriptor, instance_index=instance, reverse=False
        )
    return settings.connection.test_type_handler.create_server_client(settings)


def render(endpoint: task.Task, directory: Path) -> dict[str, Any]:
    with mock.patch(
        "tftbase.get_manifest_renderpath",
        side_effect=lambda name: str(directory / name),
    ):
        endpoint.render_pod_file("VMI")
    return yaml.safe_load((directory / f"{endpoint.pod_name}.yaml").read_text())  # type: ignore[no-any-return]


def test_config_and_pod_default() -> None:
    tc = config()
    assert tc.config.tft[0].is_vm
    reparsed = testConfig.TestConfig(
        full_config=tc.config.serialize(), kubeconfigs=("/test/kubeconfig", None)
    )
    assert reparsed.config == tc.config
    data = tc.config.serialize()
    del data["tft"][0]["is_vm"]
    pod_tc = testConfig.TestConfig(
        full_config=data, kubeconfigs=("/test/kubeconfig", None)
    )
    assert not pod_tc.config.tft[0].is_vm
    pod_tc._client_tenant = mock.Mock()
    _, client = endpoints(pod_tc)
    assert not client.is_vmi
    client.run_oc_exec("true")
    pod_tc._client_tenant.oc_exec.assert_called_once()
    with pytest.raises(ValueError, match="runtime_class_name"):
        config(runtime_class_name="kata")
    with pytest.raises(ValueError, match="vmi_template"):
        config(vmi_template="old/template")


def test_vmi_template_resources_and_overrides(tmp_path: Path) -> None:
    tc = config(
        connections=[
            {
                "server": [{"name": "worker-1"}],
                "client": [{"name": "worker-2"}],
                "cpu_request": "500m",
                "cpu_limit": "2",
                "mem_request": "2Gi",
                "mem_limit": "3Gi",
            }
        ]
    )
    server, _ = endpoints(tc)
    server._resource_name = ""
    manifest = render(server, tmp_path)
    assert manifest["kind"] == "VirtualMachineInstance"
    assert manifest["metadata"]["namespace"] == "ft"
    assert manifest["metadata"]["labels"]["tft-pod-name"] == server.pod_name
    spec = manifest["spec"]
    assert spec["nodeSelector"] == {"kubernetes.io/hostname": "worker-1"}
    assert spec["domain"]["resources"] == {
        "requests": {"cpu": "500m", "memory": "2Gi"},
        "limits": {"cpu": 2, "memory": "3Gi"},
    }
    assert spec["domain"]["memory"] == {"guest": "2Gi"}
    assert (
        spec["volumes"][0]["containerDisk"]["image"]
        == "quay.io/containerdisks/fedora:latest"
    )
    assert "imagePullSecrets" not in spec
    cloudinit = spec["volumes"][1]["cloudInitNoCloud"]
    assert yaml.safe_load(cloudinit["networkData"])["ethernets"]["ethernet"]["dhcp4"]
    assert "qemu-guest-agent" in cloudinit["userData"]
    assert "boot-finished" in spec["readinessProbe"]["exec"]["command"][-1]
    user_data = yaml.safe_load(cloudinit["userData"])
    wrapper = user_data["write_files"][0]
    assert wrapper["path"] == "/usr/local/libexec/tft-guest-exec"
    assert wrapper["permissions"] == "0755"
    assert wrapper["content"] == '#!/bin/sh\nexec "$@"\n'
    assert spec["readinessProbe"]["exec"]["command"][0] == wrapper["path"]
    assert [
        "chcon",
        "-t",
        "virt_qemu_ga_unconfined_exec_t",
        wrapper["path"],
    ] in user_data["runcmd"]
    env = {
        "TFT_VM_IMAGE": "quay.io/example/perf:1",
        "TFT_VM_CPUS": "4",
        "TFT_VM_NETWORK_BINDING": "custom",
    }
    getters = (
        tftbase.get_tft_vm_image,
        tftbase.get_tft_vm_cpus,
        tftbase.get_tft_vm_network_binding,
    )
    try:
        for getter in getters:
            getter.cache_clear()
        with mock.patch("tftbase.get_environ", side_effect=env.get):
            custom = render(server, tmp_path)["spec"]
            assert custom["domain"]["cpu"] == {"cores": 4}
            assert custom["volumes"][0]["containerDisk"]["image"] == env["TFT_VM_IMAGE"]
            assert custom["domain"]["devices"]["interfaces"][0]["binding"] == {
                "name": "custom"
            }
        override = tmp_path / "vmi.yaml.j2"
        override.write_text(
            Path(server.in_file_template).read_text() + "  hostname: overridden\n"
        )
        with mock.patch(
            "tftbase.get_tft_manifests_overrides", return_value=str(tmp_path)
        ):
            tftbase.get_manifest.cache_clear()
            overridden, _ = endpoints(tc)
            overridden._resource_name = ""
            assert render(overridden, tmp_path)["spec"]["hostname"] == "overridden"
    finally:
        tftbase.get_manifest.cache_clear()
        for getter in getters:
            getter.cache_clear()


def test_network_attachments_and_sriov(tmp_path: Path) -> None:
    tc = config(
        test_cases=[27],
        connections=[
            {
                "server": [{"name": "worker-1", "secondary_network_nad": "server-net"}],
                "client": [{"name": "worker-2", "secondary_network_nad": "client-net"}],
                "resource_name": "example.com/vf",
            }
        ],
    )
    server, client = endpoints(tc)
    spec = render(client, tmp_path)["spec"]
    assert server.node_name == client.node_name == "worker-1"
    assert spec["networks"][1]["multus"]["networkName"] == "ft/client-net"
    assert spec["domain"]["resources"]["requests"]["example.com/vf"] == "1"
    sriov_tc = config(
        test_cases=[1],
        connections=[
            {
                "server": [{"name": "worker-1"}],
                "client": [
                    {"name": "worker-2", "sriov": True, "default_network": "vf-net"}
                ],
                "resource_name": "example.com/vf",
            }
        ],
    )
    sriov_tc._client_tenant = mock.Mock()
    sriov_tc._client_tenant.oc_get.return_value = {
        "metadata": {"annotations": {kubevirt.RESOURCE_ANNOTATION: "example.com/vf"}}
    }
    _, vf_client = endpoints(sriov_tc)
    vf_spec = render(vf_client, tmp_path)["spec"]
    assert vf_spec["networks"][1]["multus"]["networkName"] == "ft/vf-net"
    assert vf_spec["domain"]["devices"]["interfaces"][1] == {
        "name": "tft-net-0",
        "sriov": {},
    }
    assert "example.com/vf" not in vf_spec["domain"]["resources"]["requests"]
    sriov_tc._client_tenant.oc_get.return_value = {}
    with pytest.raises(ValueError, match="must advertise resource_name"):
        render(vf_client, tmp_path)
    udn_tc = config(pre_provision=True, test_cases=[37, 70, 72])
    primary, _ = endpoints(udn_tc)
    secondary, _ = endpoints(udn_tc, 1)
    primary._resource_name = ""
    assert primary.pod_name == secondary.pod_name
    udn = render(primary, tmp_path)
    assert udn["metadata"]["namespace"] == "ft-udn"
    assert len(udn["spec"]["networks"]) == 3
    assert udn["spec"]["domain"]["devices"]["interfaces"][0]["binding"] == {
        "name": "l2bridge"
    }
    assert all(nad.startswith("ft-udn/") for nad in primary._vmi_secondary_nads())


def test_preprovision_reuse_and_cleanup() -> None:
    tc = config(pre_provision=True)
    descriptor = testConfig.ConfigDescriptor(tc, tft_idx=0)
    runner = trafficFlowTests.TrafficFlowTests()
    seen: list[str] = []
    with mock.patch.object(tc, "validate_selected_nodes_available"), mock.patch.object(
        task.ServerTask, "initialize"
    ), mock.patch.object(task.ClientTask, "initialize"), mock.patch.object(
        task.ServerTask, "ensure_services"
    ), mock.patch.object(
        task.ServerTask,
        "start_setup",
        autospec=True,
        side_effect=lambda self, **kw: seen.append(self.pod_name),
    ), mock.patch.object(
        task.ClientTask,
        "start_setup",
        autospec=True,
        side_effect=lambda self, **kw: seen.append(self.pod_name),
    ):
        runner._provision_all_resources(descriptor)
    s1, c1 = endpoints(tc, 0)
    s2, c2 = endpoints(tc, 1)
    assert len(seen) == len(set(seen)) == 3
    assert s1.pod_name == s2.pod_name
    assert {s1.pod_name, c1.pod_name, c2.pod_name} == set(seen)
    assert (s1.node_name, c1.node_name, c2.node_name) == (
        "worker-1",
        "worker-1",
        "worker-2",
    )
    api = mock.Mock(
        oc_get=mock.Mock(return_value=None),
        oc=mock.Mock(return_value=host.Result("", "", 0)),
    )
    endpoint = kubevirt.VmiEndpoint(api, "ft", s1.pod_name)
    with mock.patch.object(endpoint, "run", return_value=host.Result("", "", 0)):
        assert endpoint.setup("vmi.yaml", "2m")
        assert api.oc.call_args_list[0].args[0] == ["apply", "-f", "vmi.yaml"]
        api.oc_get.return_value = {"metadata": {"labels": {"tft-vmi": "true"}}}
        api.oc.reset_mock()
        assert not endpoint.setup("vmi.yaml", "2m")
        assert all(call.args[0][0] == "wait" for call in api.oc.call_args_list)
    tc._client_tenant = api
    api.oc.reset_mock()
    runner._cleanup_udn_resources(descriptor, "ft-udn", delete_namespace=False)
    assert api.oc.call_args_list[0].args[0][:4] == [
        "delete",
        kubevirt.VMI_RESOURCE,
        "-l",
        "tft-tests,tft-vmi=true",
    ]
    assert "userdefinednetwork" in api.oc.call_args_list[1].args[0]


def test_guest_exec_protocol_and_network_address() -> None:
    api = mock.Mock()
    endpoint = kubevirt.VmiEndpoint(api, "ft", "vm")
    vmi = {
        "metadata": {"uid": "uid"},
        "spec": {
            "networks": [
                {"name": "default", "pod": {}},
                {"name": "net", "multus": {"networkName": "ft/blue"}},
            ]
        },
        "status": {
            "nodeName": "worker-1",
            "interfaces": [
                {"name": "default", "ipAddress": "10.0.0.1"},
                {"name": "net", "ipAddress": "192.168.0.2"},
            ],
        },
    }
    api.oc_get.return_value = vmi
    pod = {
        "metadata": {"name": "launcher", "ownerReferences": [{"uid": "uid"}]},
        "spec": {"nodeName": "worker-1"},
        "status": {"phase": "Running"},
    }
    api.oc.side_effect = [
        host.Result(json.dumps({"items": [pod]}), "", 0),
        host.Result(json.dumps({"return": {"pid": 9}}), "", 0),
        host.Result(
            json.dumps(
                {
                    "return": {
                        "exited": True,
                        "exitcode": 7,
                        "out-data": base64.b64encode(b"hello").decode(),
                        "err-data": base64.b64encode(b"error").decode(),
                    }
                }
            ),
            "",
            0,
        ),
    ]
    result = endpoint.run(
        ["echo", "hello world", "$(literal)"], timeout=5, may_fail=True
    )
    assert result == host.Result("hello", "error", 7)
    command = api.oc.call_args_list[1].args[0]
    assert command[1:8] == [
        "exec",
        "launcher",
        "-c",
        "compute",
        "--",
        "virsh",
        "qemu-agent-command",
    ]
    request = json.loads(command[9])
    assert request["arguments"]["path"] == "/usr/local/libexec/tft-guest-exec"
    assert request["arguments"]["arg"] == [
        "/usr/bin/timeout",
        "--kill-after=5",
        "5",
        "echo",
        "hello world",
        "$(literal)",
    ]
    assert endpoint.ip() == "10.0.0.1"
    assert endpoint.ip("ft/blue") == "192.168.0.2"
    with mock.patch.object(
        endpoint, "launcher", return_value="launcher"
    ), mock.patch.object(
        endpoint, "_agent", side_effect=RuntimeError("agent disabled")
    ):
        assert endpoint.run("true", timeout=5, may_fail=True).cancelled


def test_guest_flow_lifecycle() -> None:
    _flow_lifecycle(False, "success")
    _flow_lifecycle(True, "agent-failure")
    _flow_lifecycle(False, "missing-tool")


def _flow_lifecycle(pre_provision: bool, outcome: str) -> None:
    import threading
    import tftbase

    tc = config(
        pre_provision=pre_provision, test_cases=[2 if outcome == "success" else 30]
    )
    tc._client_tenant = mock.Mock(oc=mock.Mock(return_value=host.Result("", "", 0)))
    descriptor = testConfig.ConfigDescriptor(
        tc, tft_idx=0, test_cases_idx=0, connections_idx=0
    )
    runner = trafficFlowTests.TrafficFlowTests()
    stopped = threading.Event()
    commands: list[str] = []
    summary = {"bytes": 1000000, "bits_per_second": 1000000000, "seconds": 5}
    output = json.dumps(
        {
            "start": {"tcp_mss_default": 1400},
            "end": {"sum_sent": summary, "sum_received": summary},
        }
    )

    def run(endpoint: kubevirt.VmiEndpoint, cmd: Any, **kwargs: Any) -> host.Result:
        commands.append(str(cmd))
        if "iperf3 -s" in cmd:
            stopped.wait(2)
            return host.Result("", "", 0)
        if "killall" in cmd:
            stopped.set()
            return host.Result("", "", 0)
        if "iperf3 -c" in cmd:
            if outcome == "success":
                return host.Result(output, "", 0)
            return host.Result(
                "",
                (
                    "agent transport failed"
                    if outcome == "agent-failure"
                    else "connection timed out"
                ),
                127 if outcome == "missing-tool" else 1,
                cancelled=outcome == "agent-failure",
            )
        return host.Result("LISTEN :5201", "", 0)

    with mock.patch.object(tc, "validate_selected_nodes_available"), mock.patch.object(
        task.Task, "render_pod_file"
    ), mock.patch.object(task.Task, "setup_pod") as setup, mock.patch.object(
        task.ServerTask, "ensure_services"
    ), mock.patch.object(
        task.ServerTask, "_create_multi_network_policy"
    ), mock.patch.object(
        kubevirt.VmiEndpoint, "ip", return_value="10.1.0.2"
    ), mock.patch.object(
        kubevirt.VmiEndpoint, "run", autospec=True, side_effect=run
    ):
        result = runner._run_test_case_instance(
            descriptor, 0, tftbase.TargetAccessMode.IP
        )
    assert result.flow_test.success == (outcome == "success")
    if outcome == "agent-failure":
        assert result.flow_test.msg == "agent transport failed"
    assert setup.call_count == (0 if pre_provision else 2)
    assert any("iperf3 -s" in cmd for cmd in commands)
    assert any("iperf3 -c 10.1.0.2" in cmd for cmd in commands)
    assert "killall iperf3" in commands
