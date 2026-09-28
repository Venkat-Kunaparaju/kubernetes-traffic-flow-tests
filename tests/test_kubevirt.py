# SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
# SPDX-License-Identifier: Apache-2.0

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
        "test_cases": [92, 93],
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
    reparsed = testConfig.TestConfig(
        full_config=tc.config.serialize(), kubeconfigs=("/test/kubeconfig", None)
    )
    assert reparsed.config == tc.config
    for case in tftbase.TestCaseType:
        case_tc = config(test_cases=[case.name], runtime_class_name="kata")
        case_tc._client_tenant = mock.Mock()
        server, client = endpoints(case_tc)
        for endpoint in (server, client):
            assert endpoint.is_vmi == (case.value in (92, 93))
            if endpoint.is_vmi:
                assert endpoint.get_namespace() == "ft-udn"
                assert endpoint._get_pod_runtime_class_name() is None
                assert Path(endpoint.in_file_template).name == "vmi.yaml.j2"
            else:
                assert Path(endpoint.in_file_template).name != "vmi.yaml.j2"
        if not client.is_vmi:
            client.run_oc_exec("true")
            case_tc._client_tenant.oc_exec.assert_called_once()
    connection = tc.config.serialize()["tft"][0]["connections"][0]
    for name in ("validate_offload", "ovs_doca_validate_offload", "ping_mgmt_port"):
        connection["plugins"] = [name]
        with pytest.raises(ValueError, match="Pod interface inspection"):
            config(connections=[connection])
        connection["plugins"] = [{"name": name, "test_cases": [37]}]
        config(test_cases=[37, 92], connections=[connection])
    connection["plugins"] = []
    connection["server"][0]["persistent"] = True
    for pre_provision in (False, True):
        for cases in ([92], [93], [37, 92]):
            with pytest.raises(ValueError, match="do not support persistent servers"):
                config(
                    test_cases=cases,
                    pre_provision=pre_provision,
                    connections=[connection],
                )
        pod_tc = config(
            test_cases=[37], pre_provision=pre_provision, connections=[connection]
        )
        pod_server, _ = endpoints(pod_tc)
        assert pod_server.exec_persistent


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
    assert manifest["metadata"]["namespace"] == "ft-udn"
    assert manifest["metadata"]["labels"] == {
        "tft-tests": str(server.index),
        "tft-pod-name": server.pod_name,
        "tft-vmi": "true",
    }
    spec = manifest["spec"]
    assert spec["nodeSelector"] == {"kubernetes.io/hostname": "worker-1"}
    assert spec["domain"]["resources"] == {
        "requests": {"cpu": "500m", "memory": "2Gi"},
        "limits": {"cpu": 2, "memory": "3Gi"},
    }
    assert spec["domain"]["memory"] == {"guest": "2Gi"}
    assert spec["domain"]["cpu"] == {"cores": 1}
    for cpu_request, cpu_limit, cores in (
        (None, None, 1),
        (None, "3", 3),
        (None, "1500m", 2),
        ("2", "4", 2),
        ("1500m", None, 2),
        ("1.5", None, 2),
        ("0", None, 1),
    ):
        connection = tc.config.serialize()["tft"][0]["connections"][0]
        connection.pop("cpu_request", None)
        connection.pop("cpu_limit", None)
        if cpu_request is not None:
            connection["cpu_request"] = cpu_request
        if cpu_limit is not None:
            connection["cpu_limit"] = cpu_limit
        for endpoint in endpoints(config(connections=[connection])):
            endpoint._resource_name = ""
            domain = render(endpoint, tmp_path)["spec"]["domain"]
            assert domain["cpu"] == {"cores": cores}
            request = domain["resources"]["requests"].get("cpu")
            limit = domain["resources"]["limits"].get("cpu")
            assert (
                request is None if cpu_request is None else str(request) == cpu_request
            )
            assert limit is None if cpu_limit is None else str(limit) == cpu_limit
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
        "TFT_VM_PRIMARY_INTERFACE": "binding:custom",
        "TFT_VM_SECONDARY_INTERFACE": "sriov",
    }
    getters = (tftbase.get_tft_vm_interface,)
    try:
        for getter in getters:
            getter.cache_clear()
        with mock.patch("tftbase.get_environ", side_effect=env.get):
            assert tftbase.get_tft_vm_interface(primary=False) == "sriov"
            custom = render(server, tmp_path)["spec"]
            assert custom["domain"]["cpu"] == {"cores": 1}
            assert custom["domain"]["devices"]["interfaces"][0]["binding"] == {
                "name": "custom"
            }
        for invalid in ("custom", "binding:", "binding:bad name", "binding:a:b"):
            tftbase.get_tft_vm_interface.cache_clear()
            with mock.patch("tftbase.get_environ", return_value=invalid):
                for primary in (True, False):
                    with pytest.raises(ValueError, match="bridge, sriov, or binding"):
                        tftbase.get_tft_vm_interface(primary=primary)
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
        connections=[
            {
                "server": [{"name": "worker-1", "secondary_network_nad": "server-net"}],
                "client": [{"name": "worker-2", "secondary_network_nad": "client-net"}],
                "resource_name": "example.com/vf",
            }
        ],
    )
    tc._client_tenant = mock.Mock()
    server, client = endpoints(tc)
    assert server.node_name == client.node_name == "worker-1"
    methods: dict[str, dict[str, Any]] = {
        "bridge": {"bridge": {}},
        "sriov": {"sriov": {}},
        "binding:custom": {"binding": {"name": "custom"}},
    }
    for primary_method, primary_fields in methods.items():
        for secondary_method, secondary_fields in methods.items():
            with mock.patch(
                "tftbase.get_tft_vm_interface",
                side_effect=lambda *, primary: (
                    primary_method if primary else secondary_method
                ),
            ):
                for endpoint, nad in ((server, "server-net"), (client, "client-net")):
                    spec = render(endpoint, tmp_path)["spec"]
                    assert spec["networks"] == [
                        {"name": "default", "pod": {}},
                        {
                            "name": "tft-net-0",
                            "multus": {"networkName": f"ft-udn/{nad}"},
                        },
                    ]
                    assert spec["domain"]["devices"]["interfaces"] == [
                        {"name": "default", **primary_fields},
                        {"name": "tft-net-0", **secondary_fields},
                    ]
                    assert (
                        spec["domain"]["resources"]["requests"]["example.com/vf"] == "2"
                    )
                    assert not endpoint.uses_secondary_ip
                    with mock.patch.object(
                        endpoint,
                        "_get_pod_secondary_network_nads",
                        return_value=(f"ft-udn/{nad}", "extra-ns/extra-net"),
                    ):
                        multi_spec = render(endpoint, tmp_path)["spec"]
                    assert multi_spec["networks"] == [
                        {"name": "default", "pod": {}},
                        {
                            "name": "tft-net-0",
                            "multus": {"networkName": f"ft-udn/{nad}"},
                        },
                        {
                            "name": "tft-net-1",
                            "multus": {"networkName": "extra-ns/extra-net"},
                        },
                    ]
                    assert multi_spec["domain"]["devices"]["interfaces"] == [
                        {"name": "default", **primary_fields},
                        {"name": "tft-net-0", **secondary_fields},
                        {"name": "tft-net-1", **secondary_fields},
                    ]
                    for resource_type in ("requests", "limits"):
                        assert (
                            multi_spec["domain"]["resources"][resource_type][
                                "example.com/vf"
                            ]
                            == "3"
                        )
    tc._client_tenant.oc_get.assert_not_called()
    with mock.patch("tftbase.get_tft_vm_interface", return_value="bridge"):
        sriov_tc = config(
            connections=[
                {
                    "server": [{"name": "worker-1", "sriov": True}],
                    "client": [{"name": "worker-2"}],
                }
            ]
        )
        sriov_server, _ = endpoints(sriov_tc)
        sriov_server._resource_name = ""
        sriov_spec = render(sriov_server, tmp_path)["spec"]
        assert sriov_spec["networks"] == [{"name": "default", "pod": {}}]
        assert sriov_spec["domain"]["devices"]["interfaces"] == [
            {"name": "default", "bridge": {}}
        ]


def test_preprovision_reuse_and_cleanup() -> None:
    tc = config(pre_provision=True, test_cases=[92, 93, 37, 38, 70])
    descriptor = testConfig.ConfigDescriptor(tc, tft_idx=0)
    runner = trafficFlowTests.TrafficFlowTests()
    runner._udn_setup_done = True
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
    assert len(seen) == len(set(seen)) == 6
    vm_names = {name for name in seen if name.startswith("tft-vmi-")}
    assert len(vm_names) == 3
    assert s1.pod_name == s2.pod_name
    assert {s1.pod_name, c1.pod_name, c2.pod_name} == vm_names
    pod_server, _ = endpoints(tc, 2)
    assert not pod_server.is_vmi
    assert not s1._get_pod_secondary_network_nads()
    assert pod_server._get_pod_secondary_network_nads() == ("ft-udn/tft-cudn-layer3",)
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
        endpoint.setup("vmi.yaml", "2m")
        assert api.oc.call_args_list[0].args[0] == ["apply", "-f", "vmi.yaml"]
        api.oc_get.return_value = {"metadata": {"labels": {"tft-vmi": "true"}}}
        api.oc.reset_mock()
        endpoint.setup("vmi.yaml", "2m")
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
                {"name": "net", "ipAddress": "192.168.0.2"},
                {"name": "default", "ipAddress": "10.0.0.1"},
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
    with mock.patch.object(
        endpoint, "launcher", return_value="launcher"
    ), mock.patch.object(
        endpoint,
        "_agent",
        side_effect=[{"pid": 9}, {"exited": True, "signal": 15}],
    ):
        result = endpoint.run("true", timeout=5, may_fail=True)
        assert result == host.Result("", "", 15)
        assert not result.success
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

    tc = config(pre_provision=pre_provision, test_cases=[93])
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
                    else "iperf3: command not found"
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
        assert "agent transport failed" in (result.flow_test.msg or "")
    if outcome == "missing-tool":
        assert "iperf3: command not found" in (result.flow_test.msg or "")
    assert setup.call_count == (0 if pre_provision else 2)
    assert any("iperf3 -s" in cmd for cmd in commands)
    assert any("iperf3 -c 10.1.0.2" in cmd for cmd in commands)
    assert "killall iperf3" in commands
