"""只读诊断：定位补跑 Job 最近一次可见的非零退出并取回日志。

流程（全程只读，不改集群状态）：
1. 按 (namespace, job 名) 读 Job 并核对 UID，防止查到同名重建的 Job；
2. 列出命名空间 Pod，按 ownerReferences(Job, uid) 过滤出该 Job 控制的 Pod，
   只看单容器 Pod（多容器 Pod 记入 skipped_pods）；
3. 对每个容器同时考察当前退出（state.terminated）与上一次退出
   （lastState.terminated），收集 exitCode != 0 且 finishedAt 可见的记录，
   按结束时间取最近一次；
4. 取该容器最后 80 行日志（上一次退出用 previous=true），取完后重新读 Pod
   复核 containerID：只有确认日志仍属于该次退出才附上；
   取日志途中容器再次重启、Pod 被清理或日志接口报错时，保留退出信息，
   logs 置空并说明原因。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .kube import KubeClient, KubeError, KubeNotFound
from .service import ServiceError

LOG_TAIL_LINES = 80


@dataclass
class TerminationCandidate:
    """一次可见的非零退出（来自某个单容器 Pod）。"""

    pod: str
    container: str
    exit_code: int
    finished_at: datetime
    finished_at_raw: str
    source: str  # "current"（state.terminated）或 "previous"（lastState.terminated）
    container_id: str | None
    restart_count: int
    terminated_raw: dict[str, Any]  # 原始 terminated 字典，复核时比对用


def _parse_rfc3339(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _owned_by_job(pod: dict[str, Any], job_uid: str) -> bool:
    for owner in pod.get("metadata", {}).get("ownerReferences", []) or []:
        if owner.get("kind") == "Job" and owner.get("uid") == job_uid:
            return True
    return False


def _container_status(pod: dict[str, Any], container_name: str) -> dict | None:
    for cs in pod.get("status", {}).get("containerStatuses", []) or []:
        if cs.get("name") == container_name:
            return cs
    return None


def _candidate_from(
    pod_name: str,
    container_name: str,
    restart_count: int,
    terminated: dict[str, Any] | None,
    source: str,
) -> TerminationCandidate | None:
    """从 terminated 字典构造候选；非零退出且结束时间可见才有效。"""
    if not terminated:
        return None
    exit_code = terminated.get("exitCode")
    if not isinstance(exit_code, int) or exit_code == 0:
        return None
    finished_raw = terminated.get("finishedAt")
    finished = _parse_rfc3339(finished_raw)
    if finished is None:
        # 结束时间不可见，无法排序，跳过
        return None
    return TerminationCandidate(
        pod=pod_name,
        container=container_name,
        exit_code=exit_code,
        finished_at=finished,
        finished_at_raw=finished_raw,
        source=source,
        container_id=terminated.get("containerID"),
        restart_count=restart_count,
        terminated_raw=terminated,
    )


def _same_termination(recorded: dict[str, Any], current: dict[str, Any] | None) -> bool:
    """复核用：取日志后读到的 terminated 是否仍是记录的那次退出。

    优先比 containerID（最可靠）；缺 containerID 时退化为
    (exitCode, finishedAt) 组合比对。
    """
    if not current:
        return False
    cid_a = recorded.get("containerID")
    cid_b = current.get("containerID")
    if cid_a or cid_b:
        return bool(cid_a) and cid_a == cid_b
    return (
        recorded.get("exitCode") == current.get("exitCode")
        and recorded.get("finishedAt") == current.get("finishedAt")
    )


class DiagnoseService:
    def __init__(self, kube: KubeClient):
        self.kube = kube

    async def last_failure(
        self, namespace: str, job_name: str, job_uid: str
    ) -> dict[str, Any]:
        # 1. 确认 Job 身份（名 + UID），防止查到同名重建的 Job
        try:
            job = await self.kube.get_job(namespace, job_name)
        except KubeNotFound as exc:
            raise ServiceError(
                f"Job {namespace}/{job_name} 不存在", status_code=404
            ) from exc
        except KubeError as exc:
            raise ServiceError(f"读取 Job 失败: {exc}", status_code=502) from exc
        actual_uid = job.get("metadata", {}).get("uid")
        if actual_uid != job_uid:
            raise ServiceError(
                f"Job {namespace}/{job_name} 的 UID 为 {actual_uid}，"
                f"与输入的 {job_uid} 不一致（Job 可能已被删除重建，请核对后再查）",
                status_code=409,
            )

        # 2. 列 Pod；列表都查不到时与“没有可见失败”分开提示
        try:
            pods = await self.kube.list_pods(namespace)
        except KubeError as exc:
            raise ServiceError(
                f"无法获取命名空间 {namespace} 的 Pod 列表，诊断中止: {exc}",
                status_code=502,
            ) from exc

        # 3. 只保留该 Job 控制的单容器 Pod
        owned = [p for p in pods if _owned_by_job(p, job_uid)]
        skipped: list[str] = []
        candidates: list[TerminationCandidate] = []
        checked = 0
        for pod in owned:
            pod_name = pod.get("metadata", {}).get("name", "?")
            containers = pod.get("spec", {}).get("containers", []) or []
            if len(containers) != 1:
                skipped.append(pod_name)
                continue
            container_name = containers[0].get("name", "")
            cs = _container_status(pod, container_name)
            if cs is None:
                continue
            checked += 1
            restart_count = cs.get("restartCount", 0)
            state = cs.get("state", {}) or {}
            last_state = cs.get("lastState", {}) or {}
            for terminated, source in (
                (state.get("terminated"), "current"),
                (last_state.get("terminated"), "previous"),
            ):
                cand = _candidate_from(
                    pod_name, container_name, restart_count, terminated, source
                )
                if cand is not None:
                    candidates.append(cand)

        base: dict[str, Any] = {
            "namespace": namespace,
            "job_name": job_name,
            "job_uid": job_uid,
            "checked_pods": checked,
            "skipped_pods": skipped,
        }

        # 4. 没有可见失败（与“查不到 Pod 列表”分开提示）
        if not candidates:
            return {
                "status": "no_visible_failure",
                **base,
                "pod": None,
                "container": None,
                "exit_code": None,
                "finished_at": None,
                "termination_source": None,
                "restart_count": None,
                "logs": None,
                "logs_status": "not_applicable",
                "logs_reason": None,
                "message": (
                    f"Job 控制的 {checked} 个单容器 Pod 中没有可见的非零退出"
                    + (f"（另有 {len(skipped)} 个多容器 Pod 未纳入检查）" if skipped else "")
                    + "；若 Pod 已被清理则其退出记录不可见"
                ),
            }

        # 5. 按结束时间取最近一次
        latest = max(
            candidates,
            key=lambda c: (c.finished_at, c.pod, c.source),
        )

        # 6. 取日志并复核归属
        logs, logs_status, logs_reason = await self._fetch_logs_confirmed(
            namespace, latest
        )

        return {
            "status": "found",
            **base,
            "pod": latest.pod,
            "container": latest.container,
            "exit_code": latest.exit_code,
            "finished_at": latest.finished_at,
            "termination_source": latest.source,
            "restart_count": latest.restart_count,
            "logs": logs,
            "logs_status": logs_status,
            "logs_reason": logs_reason,
            "message": None,
        }

    async def _fetch_logs_confirmed(
        self, namespace: str, cand: TerminationCandidate
    ) -> tuple[str | None, str, str | None]:
        """取日志并复核它仍属于记录的那次退出。

        返回 (logs, logs_status, logs_reason)：
        - attached：复核通过，日志确认属于该次退出；
        - unavailable：容器再重启 / Pod 被清理 / 日志接口报错 / 状态复核失败，
          退出信息保留但日志不附。
        """
        previous = cand.source == "previous"
        try:
            logs = await self.kube.get_pod_log(
                namespace,
                cand.pod,
                cand.container,
                tail_lines=LOG_TAIL_LINES,
                previous=previous,
            )
        except KubeNotFound:
            return (
                None,
                "unavailable",
                f"取日志时 Pod {cand.pod} 已被清理（404），无法取回该次退出的日志",
            )
        except KubeError as exc:
            return (
                None,
                "unavailable",
                f"日志接口报错，无法取回该次退出的日志: {exc}",
            )

        # 取到日志后重新读 Pod，确认对应退出记录没变（容器没有再重启）
        try:
            pod = await self.kube.get_pod(namespace, cand.pod)
        except KubeNotFound:
            return (
                None,
                "unavailable",
                f"取日志后复核时 Pod {cand.pod} 已被清理（404），"
                "日志归属无法确认，未附上",
            )
        except KubeError as exc:
            return (
                None,
                "unavailable",
                f"取日志后复核 Pod 状态失败，日志归属无法确认，未附上: {exc}",
            )

        cs = _container_status(pod, cand.container)
        if cs is None:
            return (
                None,
                "unavailable",
                f"复核时 Pod {cand.pod} 中找不到容器 {cand.container} 的状态，"
                "日志归属无法确认，未附上",
            )
        if previous:
            current_term = (cs.get("lastState", {}) or {}).get("terminated")
        else:
            current_term = (cs.get("state", {}) or {}).get("terminated")
        if not _same_termination(cand.terminated_raw, current_term):
            return (
                None,
                "unavailable",
                "取日志期间容器再次重启（退出记录已变化），"
                "取到的日志可能不属于该次退出，未附上",
            )

        # 客户端再截一次最后 80 行，防止上游未按 tailLines 截断
        lines = logs.splitlines()
        if len(lines) > LOG_TAIL_LINES:
            lines = lines[-LOG_TAIL_LINES:]
        return "\n".join(lines), "attached", None
