"""本地 Kubernetes API 模拟服务（batch/v1 子集 + core/v1 Pod 只读子集）。

用途：
- 提供 CronJob / Job 的读取、列表、创建，行为贴近 1.32 的标签与注解；
- 提供 Pod 列表/读取与容器日志（core/v1），支撑补跑 Job 失败诊断演示；
- 通过请求头注入故障，复现“创建成功后回包超时”：
  * X-Mock-Fault: create-timeout        —— 已落库但延迟到客户端超时；
  * X-Mock-Fault: create-timeout-lose   —— 已落库但直接断开连接（彻底无回包）；
  * X-Mock-Fault: create-conflict-once  —— 第一次创建返回 409，之后正常。
- 诊断相关故障（请求头 X-Mock-Pod-Fault，仅作用于 Pod/日志接口）：
  * pods-list-error   —— 列 Pod 返回 500；
  * log-error         —— 日志接口返回 500；
  * pod-gone-on-log   —— 取日志时 Pod 已被清理（删 Pod 并返回 404）。
- 种子数据覆盖：暂停的每日巡检 Job、带 DST 的纽约时区 Job、未暂停 Job、无时区 Job；
  另预置三个补跑 Job 及其 Pod，用于失败诊断演示：
  * inspection-daily-catchup-202611190100 —— 最近一次失败是某 Pod 的“上一次退出”；
  * inspection-daily-catchup-flaky        —— 取日志途中容器再次重启（注解触发）；
  * inspection-daily-catchup-202611170100 —— 全部成功，演示“没有可见失败”。
"""
from __future__ import annotations

import asyncio
import copy
import os
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from app.kube import (
    ANN_CRONJOB_UID,
    ANN_JOB_NAME,
    LABEL_CRONJOB_NAME,
)

API_PREFIX = "/apis/batch/v1"
CORE_PREFIX = "/api/v1"

# 注解触发：对该 Pod 取当前日志时，模拟“取日志途中容器又重启了一次”
ANN_RESTART_ON_LOG = "mock.example.com/restart-on-log"


def rfc3339_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _uid(prefix: str, seed: str) -> str:
    # 确定性 UID，方便测试断言
    return f"{prefix}-{uuid.uuid5(uuid.NAMESPACE_URL, seed)}"


def seed_state() -> dict:
    cj_daily = {
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": {
            "name": "inspection-daily",
            "namespace": "default",
            "uid": _uid("cronjob", "inspection-daily"),
            "resourceVersion": "1",
        },
        "spec": {
            "schedule": "0 9 * * *",
            "timeZone": "Asia/Shanghai",
            "suspend": True,
            "concurrencyPolicy": "Forbid",
            "jobTemplate": {
                "metadata": {"labels": {"app": "inspection"}},
                "spec": {
                    "template": {
                        "metadata": {"labels": {"app": "inspection"}},
                        "spec": {
                            "restartPolicy": "OnFailure",
                            "containers": [
                                {"name": "worker", "image": "busybox:1.36",
                                 "command": ["sh", "-c", "echo inspect"]}
                            ],
                        },
                    }
                },
            },
        },
    }
    cj_dst = {
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": {
            "name": "inspection-ny",
            "namespace": "default",
            "uid": _uid("cronjob", "inspection-ny"),
            "resourceVersion": "1",
        },
        "spec": {
            "schedule": "30 1 * * *",
            "timeZone": "America/New_York",
            "suspend": True,
            "jobTemplate": {
                "metadata": {"labels": {"app": "inspection-ny"}},
                "spec": {
                    "template": {
                        "spec": {
                            "restartPolicy": "Never",
                            "containers": [
                                {"name": "worker", "image": "busybox:1.36",
                                 "command": ["sh", "-c", "echo ny"]}
                            ],
                        }
                    }
                },
            },
        },
    }
    cj_running = copy.deepcopy(cj_daily)
    cj_running["metadata"]["name"] = "inspection-running"
    cj_running["metadata"]["uid"] = _uid("cronjob", "inspection-running")
    cj_running["spec"]["suspend"] = False

    cj_no_tz = copy.deepcopy(cj_daily)
    cj_no_tz["metadata"]["name"] = "legacy-no-tz"
    cj_no_tz["metadata"]["uid"] = _uid("cronjob", "legacy-no-tz")
    del cj_no_tz["spec"]["timeZone"]

    cj_weekly = copy.deepcopy(cj_daily)
    cj_weekly["metadata"]["name"] = "inspection-weekly"
    cj_weekly["metadata"]["uid"] = _uid("cronjob", "inspection-weekly")
    cj_weekly["spec"]["schedule"] = "0 9 * * MON"

    cj_utc = copy.deepcopy(cj_daily)
    cj_utc["metadata"]["name"] = "inspection-utc"
    cj_utc["metadata"]["uid"] = _uid("cronjob", "inspection-utc")
    cj_utc["spec"]["schedule"] = "30 0 * * *"
    cj_utc["spec"]["timeZone"] = "UTC"
    # 无任何预置 Job，用于窗口/到点边界测试

    cronjobs = {
        ("default", "inspection-daily"): cj_daily,
        ("default", "inspection-ny"): cj_dst,
        ("default", "inspection-running"): cj_running,
        ("default", "legacy-no-tz"): cj_no_tz,
        ("default", "inspection-weekly"): cj_weekly,
        ("default", "inspection-utc"): cj_utc,
    }

    # 原调度器已为 daily 建过一轮 Job（1.32 风格标签/注解）
    scheduler_job = _scheduler_job(cj_daily, "2026-11-20T01:00:00Z")
    jobs: dict[tuple[str, str], dict] = {
        ("default", scheduler_job["metadata"]["name"]): scheduler_job
    }

    # ---- 失败诊断演示：三个历史补跑 Job 及其 Pod/日志 ----
    # 1) 最近一次失败是某 Pod 的“上一次退出”（lastState.terminated）
    demo_job = _catchup_job(
        cj_daily, "2026-11-19T01:00:00Z", "inspection-daily-catchup-202611190100"
    )
    jobs[("default", demo_job["metadata"]["name"])] = demo_job
    demo_uid = demo_job["metadata"]["uid"]
    demo_name = demo_job["metadata"]["name"]
    # 2) 取日志途中容器再次重启（注解 ANN_RESTART_ON_LOG 触发）
    flaky_job = _catchup_job(
        cj_daily, "2026-11-18T01:00:00Z", "inspection-daily-catchup-flaky"
    )
    jobs[("default", flaky_job["metadata"]["name"])] = flaky_job
    flaky_uid = flaky_job["metadata"]["uid"]
    flaky_name = flaky_job["metadata"]["name"]
    # 3) 全部成功，演示“没有可见失败”
    ok_job = _catchup_job(
        cj_daily, "2026-11-17T01:00:00Z", "inspection-daily-catchup-202611170100"
    )
    jobs[("default", ok_job["metadata"]["name"])] = ok_job
    ok_uid = ok_job["metadata"]["uid"]
    ok_name = ok_job["metadata"]["name"]

    pods: dict[tuple[str, str], dict] = {}
    logs: dict[tuple[str, str, str], str] = {}

    def add_pod(pod: dict, pod_logs: dict[str, str]) -> None:
        """pod_logs: {containerID: 日志文本}"""
        pods[("default", pod["metadata"]["name"])] = pod
        for cid, text in pod_logs.items():
            logs[("default", pod["metadata"]["name"], cid)] = text

    # Job 1 的 Pod p1：当前在跑（第 3 次实例），上一次退出非零（01:05:00Z 结束）
    add_pod(
        _demo_pod(
            demo_name, demo_uid, "p1",
            container_status={
                "name": "worker",
                "containerID": "containerd://c-p1-run3",
                "restartCount": 2,
                "state": {"running": {"startedAt": "2026-11-20T01:05:20Z"}},
                "lastState": {"terminated": {
                    "exitCode": 1, "reason": "Error",
                    "startedAt": "2026-11-20T01:04:40Z",
                    "finishedAt": "2026-11-20T01:05:00Z",
                    "containerID": "containerd://c-p1-run2",
                }},
            },
            phase="Running",
        ),
        {
            "containerd://c-p1-run2": _make_logs(
                "c-p1-run2", 100,
                "ERROR: inspection template check failed: "
                "disk latency 990ms > threshold 200ms (exit 1)",
            ),
            "containerd://c-p1-run3": _make_logs(
                "c-p1-run3", 5, "still running ...", final=False
            ),
        },
    )
    # Job 1 的 Pod p2：当前已终止（01:03:00Z 结束，比 p1 的上一次退出更早）
    add_pod(
        _demo_pod(
            demo_name, demo_uid, "p2",
            container_status={
                "name": "worker",
                "containerID": "containerd://c-p2-run1",
                "restartCount": 0,
                "state": {"terminated": {
                    "exitCode": 2, "reason": "Error",
                    "startedAt": "2026-11-20T01:02:30Z",
                    "finishedAt": "2026-11-20T01:03:00Z",
                    "containerID": "containerd://c-p2-run1",
                }},
            },
            phase="Failed",
        ),
        {
            "containerd://c-p2-run1": _make_logs(
                "c-p2-run1", 20, "ERROR: config reload failed (exit 2)"
            ),
        },
    )
    # Job 1 的 Pod multi：多容器，诊断时应被跳过
    multi = _demo_pod(
        demo_name, demo_uid, "multi",
        container_status=None, phase="Running",
    )
    multi["spec"]["containers"].append(
        {"name": "sidecar", "image": "busybox:1.36", "command": ["sh", "-c", "sleep 3600"]}
    )
    multi["status"]["containerStatuses"] = [
        {"name": "worker", "containerID": "containerd://c-multi-w",
         "restartCount": 0, "state": {"running": {"startedAt": "2026-11-20T01:01:00Z"}}},
        {"name": "sidecar", "containerID": "containerd://c-multi-s",
         "restartCount": 0, "state": {"running": {"startedAt": "2026-11-20T01:01:00Z"}}},
    ]
    pods[("default", multi["metadata"]["name"])] = multi

    # Job 2 的 Pod：当前已终止；带注解，取日志时模拟“又重启了一次”
    add_pod(
        _demo_pod(
            flaky_name, flaky_uid, "p1",
            container_status={
                "name": "worker",
                "containerID": "containerd://c-flaky-run1",
                "restartCount": 0,
                "state": {"terminated": {
                    "exitCode": 1, "reason": "Error",
                    "startedAt": "2026-11-20T01:09:30Z",
                    "finishedAt": "2026-11-20T01:10:00Z",
                    "containerID": "containerd://c-flaky-run1",
                }},
            },
            phase="Failed",
            extra_annotations={ANN_RESTART_ON_LOG: "true"},
        ),
        {
            "containerd://c-flaky-run1": _make_logs(
                "c-flaky-run1", 30, "ERROR: flaky inspection failed (exit 1)"
            ),
        },
    )

    # Job 3 的 Pod：成功退出（exit 0），不应被当作失败
    add_pod(
        _demo_pod(
            ok_name, ok_uid, "p1",
            container_status={
                "name": "worker",
                "containerID": "containerd://c-ok-run1",
                "restartCount": 0,
                "state": {"terminated": {
                    "exitCode": 0, "reason": "Completed",
                    "startedAt": "2026-11-17T01:00:10Z",
                    "finishedAt": "2026-11-17T01:00:40Z",
                    "containerID": "containerd://c-ok-run1",
                }},
            },
            phase="Succeeded",
        ),
        {"containerd://c-ok-run1": _make_logs("c-ok-run1", 10, "inspection ok", final=False)},
    )

    return {
        "cronjobs": cronjobs,
        "jobs": jobs,
        "pods": pods,
        "logs": logs,
        "conflict_used": set(),
    }


def _catchup_job(cronjob: dict, scheduled_iso: str, name: str) -> dict:
    """构造一个“本服务补建”风格的历史 Job（带 catchup 标签与 UID 注解）。"""
    cj_name = cronjob["metadata"]["name"]
    cj_uid = cronjob["metadata"]["uid"]
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name,
            "namespace": "default",
            "uid": _uid("job", name),
            "creationTimestamp": rfc3339_now(),
            "resourceVersion": "3",
            "labels": {
                LABEL_CRONJOB_NAME: cj_name,
                "inspection.example.com/catchup": "true",
                "app": "inspection",
            },
            "annotations": {
                ANN_CRONJOB_UID: cj_uid,
                ANN_JOB_NAME: scheduled_iso,
                "inspection.example.com/cronjob-uid": cj_uid,
                "inspection.example.com/scheduled-at": scheduled_iso,
            },
            "ownerReferences": [
                {"apiVersion": "batch/v1", "kind": "CronJob",
                 "name": cj_name, "uid": cj_uid}
            ],
        },
        "spec": copy.deepcopy(cronjob["spec"]["jobTemplate"]["spec"]),
    }


def _demo_pod(
    job_name: str,
    job_uid: str,
    suffix: str,
    container_status: dict | None,
    phase: str,
    extra_annotations: dict | None = None,
) -> dict:
    name = f"{job_name}-{suffix}"
    metadata = {
        "name": name,
        "namespace": "default",
        "uid": _uid("pod", name),
        "creationTimestamp": rfc3339_now(),
        "resourceVersion": "5",
        "labels": {
            "app": "inspection",
            "job-name": job_name,
            "batch.kubernetes.io/job-name": job_name,
        },
        "ownerReferences": [
            {"apiVersion": "batch/v1", "kind": "Job", "name": job_name,
             "uid": job_uid, "controller": True, "blockOwnerDeletion": True}
        ],
    }
    if extra_annotations:
        metadata["annotations"] = dict(extra_annotations)
    statuses = [container_status] if container_status else []
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": metadata,
        "spec": {
            "restartPolicy": "OnFailure",
            "containers": [
                {"name": "worker", "image": "busybox:1.36",
                 "command": ["sh", "-c", "run-inspection"]}
            ],
        },
        "status": {"phase": phase, "containerStatuses": statuses},
    }


def _make_logs(container_tag: str, lines: int, final_line: str, final: bool = True) -> str:
    """生成带容器标识的演示日志（便于核对日志属于哪个容器实例）。"""
    out = []
    for i in range(1, lines + 1):
        out.append(
            f"2026-11-20T01:04:{i % 60:02d}Z [worker@{container_tag}] "
            f"inspection step {i:03d}/{lines}: probe ok"
        )
    if final:
        out.append(f"2026-11-20T01:05:00Z [worker@{container_tag}] {final_line}")
    return "\n".join(out) + "\n"


def _scheduler_job(cronjob: dict, scheduled_iso: str) -> dict:
    cj_name = cronjob["metadata"]["name"]
    cj_uid = cronjob["metadata"]["uid"]
    stamp = scheduled_iso.replace("-", "").replace(":", "").replace("T", "")[:10]
    name = f"{cj_name}-{stamp}"
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name,
            "namespace": "default",
            "uid": _uid("job", name),
            "creationTimestamp": rfc3339_now(),
            "resourceVersion": "2",
            "labels": {
                LABEL_CRONJOB_NAME: cj_name,
                **cronjob["spec"]["jobTemplate"].get("metadata", {})
                .get("labels", {}),
            },
            "annotations": {
                ANN_CRONJOB_UID: cj_uid,
                ANN_JOB_NAME: scheduled_iso,
            },
            "ownerReferences": [
                {"apiVersion": "batch/v1", "kind": "CronJob",
                 "name": cj_name, "uid": cj_uid}
            ],
        },
        "spec": copy.deepcopy(cronjob["spec"]["jobTemplate"]["spec"]),
    }


def create_mock_app(state: dict | None = None) -> FastAPI:
    app = FastAPI(title="Kubernetes API 模拟服务", version="1.32-mock")
    app.state.store = state or seed_state()
    # 模拟服务端处理延迟（秒），故障时配合使用
    app.state.slow_delay = float(os.environ.get("MOCK_SLOW_DELAY", "8"))

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    @app.get(f"{API_PREFIX}/namespaces/{{namespace}}/cronjobs/{{name}}")
    async def get_cronjob(namespace: str, name: str):
        obj = app.state.store["cronjobs"].get((namespace, name))
        if obj is None:
            return _status(404, "NotFound", f"cronjobs {name} not found")
        return copy.deepcopy(obj)

    @app.get(f"{API_PREFIX}/namespaces/{{namespace}}/cronjobs")
    async def list_cronjobs(namespace: str):
        items = [
            copy.deepcopy(cj)
            for (ns, _), cj in app.state.store["cronjobs"].items()
            if ns == namespace
        ]
        return {"apiVersion": "batch/v1", "kind": "CronJobList", "items": items}

    @app.get(f"{API_PREFIX}/namespaces/{{namespace}}/jobs")
    async def list_jobs(namespace: str):
        items = [
            copy.deepcopy(job)
            for (ns, _), job in app.state.store["jobs"].items()
            if ns == namespace
        ]
        return {"apiVersion": "batch/v1", "kind": "JobList", "items": items}

    @app.get(f"{API_PREFIX}/namespaces/{{namespace}}/jobs/{{name}}")
    async def get_job(namespace: str, name: str):
        obj = app.state.store["jobs"].get((namespace, name))
        if obj is None:
            return _status(404, "NotFound", f"jobs {name} not found")
        return copy.deepcopy(obj)

    @app.post(f"{API_PREFIX}/namespaces/{{namespace}}/jobs")
    async def create_job(namespace: str, request: Request):
        body = await request.json()
        name = body["metadata"]["name"]
        key = (namespace, name)
        fault = request.headers.get("X-Mock-Fault", "")

        # 409 一次：模拟同名并发冲突
        if fault == "create-conflict-once" and key not in app.state.store["conflict_used"]:
            app.state.store["conflict_used"].add(key)
            return _status(409, "AlreadyExists", f"jobs {name} already exists")

        if key in app.state.store["jobs"]:
            return _status(409, "AlreadyExists", f"jobs {name} already exists")

        stored = copy.deepcopy(body)
        stored["metadata"].setdefault("uid", str(uuid.uuid4()))
        stored["metadata"].setdefault("creationTimestamp", rfc3339_now())
        stored["metadata"].setdefault("resourceVersion", "1")
        app.state.store["jobs"][key] = stored

        if fault in ("create-timeout", "create-timeout-lose"):
            # 关键：对象已落库，再制造回包超时（滞留时间由 MOCK_SLOW_DELAY 控制，
            # 需大于客户端 KUBE_CREATE_TIMEOUT）。
            # ASGI/HTTP 下客户端会先超时断开；lose 模式在断开后直接吞掉响应。
            await asyncio.sleep(app.state.slow_delay)
            if fault == "create-timeout-lose":
                # 模拟连接被对端静默丢弃：返回非标准网关错误且无业务体
                return JSONResponse(
                    status_code=599,
                    content={"reason": "ConnectionLost", "message": "response lost"},
                )
            return _status(504, "Timeout", "simulated gateway timeout after persist")

        return JSONResponse(status_code=201, content=copy.deepcopy(stored))

    # ---------- core/v1：Pod 与日志（只读诊断用） ----------

    @app.get(f"{CORE_PREFIX}/namespaces/{{namespace}}/pods")
    async def list_pods(namespace: str, request: Request):
        if request.headers.get("X-Mock-Pod-Fault") == "pods-list-error":
            return _status(500, "InternalError", "simulated pod list failure")
        items = [
            copy.deepcopy(pod)
            for (ns, _), pod in app.state.store["pods"].items()
            if ns == namespace
        ]
        return {"apiVersion": "v1", "kind": "PodList", "items": items}

    @app.get(f"{CORE_PREFIX}/namespaces/{{namespace}}/pods/{{name}}")
    async def get_pod(namespace: str, name: str):
        obj = app.state.store["pods"].get((namespace, name))
        if obj is None:
            return _status(404, "NotFound", f"pods {name} not found")
        return copy.deepcopy(obj)

    @app.get(f"{CORE_PREFIX}/namespaces/{{namespace}}/pods/{{name}}/log")
    async def get_pod_log(
        namespace: str,
        name: str,
        request: Request,
        container: str = "",
        tailLines: int = 80,
        previous: bool = False,
    ):
        fault = request.headers.get("X-Mock-Pod-Fault")
        if fault == "log-error":
            return _status(500, "InternalError", "simulated log backend failure")
        key = (namespace, name)
        pod = app.state.store["pods"].get(key)
        if pod is None:
            return _status(404, "NotFound", f"pods {name} not found")
        if fault == "pod-gone-on-log":
            # 取日志的瞬间 Pod 被清理：删除并 404，后续读 Pod 同样 404
            del app.state.store["pods"][key]
            return _status(404, "NotFound", f"pods {name} not found")

        cs = next(
            (c for c in pod.get("status", {}).get("containerStatuses", [])
             if c.get("name") == container),
            None,
        )
        if cs is None:
            return _status(
                400, "BadRequest", f"container {container} not found in pod {name}"
            )

        if previous:
            term = (cs.get("lastState") or {}).get("terminated")
            if not term:
                return _status(
                    400, "BadRequest",
                    f"previous terminated container {container} not found in pod {name}",
                )
            cid = term.get("containerID")
        else:
            state = cs.get("state") or {}
            if state.get("terminated"):
                cid = state["terminated"].get("containerID")
                # 演示故障：取当前日志的途中，容器又重启了一次。
                # 先翻转状态（旧退出挪入 lastState），日志仍按旧实例返回，
                # 调用方复核 Pod 时即可发现退出记录已变化。
                ann = pod.get("metadata", {}).get("annotations", {}) or {}
                if ann.get(ANN_RESTART_ON_LOG) == "true":
                    new_cid = f"containerd://{name}-run{cs.get('restartCount', 0) + 2}"
                    cs["lastState"] = {"terminated": copy.deepcopy(state["terminated"])}
                    cs["state"] = {"running": {"startedAt": rfc3339_now()}}
                    cs["containerID"] = new_cid
                    cs["restartCount"] = cs.get("restartCount", 0) + 1
                    pod["status"]["phase"] = "Running"
            else:
                # running 容器：当前实例 ID 在 containerStatus 顶层
                cid = cs.get("containerID")

        text = app.state.store["logs"].get((namespace, name, cid), "")
        lines = text.splitlines()
        if tailLines and len(lines) > tailLines:
            lines = lines[-tailLines:]
        return PlainTextResponse("\n".join(lines) + ("\n" if lines else ""))

    @app.post("/_mock/reset")
    async def reset():
        app.state.store = seed_state()
        return {"reset": True}

    return app


def _status(code: int, reason: str, message: str) -> JSONResponse:
    return JSONResponse(
        status_code=code,
        content={
            "kind": "Status",
            "apiVersion": "v1",
            "metadata": {},
            "status": "Failure",
            "reason": reason,
            "message": message,
            "code": code,
        },
    )


app = create_mock_app()
