"""Pydantic 响应模型（Swagger 文档用）。"""
from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class JobMatchSource(str, Enum):
    scheduler = "scheduler"   # 原调度器创建的 Job
    catchup = "catchup"       # 本服务补建的 Job


class ScheduleResponse(BaseModel):
    namespace: str
    cronjob: str
    cronjob_uid: str
    time_zone: str
    schedule: str
    suspended: bool
    scheduled_at_utc: datetime = Field(..., description="最近一次应到点的计划时刻（UTC）")
    is_daily_fixed_time: bool
    existing_job: dict | None = Field(
        None, description="已存在的匹配 Job（原调度器或补跑 Job），无则 null"
    )
    existing_job_source: JobMatchSource | None = None
    within_catchup_window: bool = Field(..., description="距到点是否在 2 小时补跑期限内（UTC）")


class CatchupStatus(str, Enum):
    returned_existing = "returned_existing"
    created = "created"
    pending_verification = "pending_verification"
    rejected = "rejected"


class CatchupResponse(BaseModel):
    status: CatchupStatus
    namespace: str
    cronjob: str
    cronjob_uid: str
    scheduled_at_utc: datetime
    job: dict | None = None
    job_source: JobMatchSource | None = None
    reason: str | None = Field(None, description="拒绝或待核实原因")
    record_id: int | None = Field(None, description="本地补跑台账记录 ID")


class RecordResponse(BaseModel):
    id: int
    namespace: str
    cronjob_name: str
    cronjob_uid: str
    scheduled_at: datetime
    job_name: str | None
    job_uid: str | None
    source: str
    state: str
    detail: str | None
    created_at: datetime
    updated_at: datetime


class HealthResponse(BaseModel):
    status: str
    kubernetes_api: str


class LastFailureStatus(str, Enum):
    found = "found"                            # 找到最近一次可见的非零退出
    no_visible_failure = "no_visible_failure"  # 没有可见的非零退出


class TerminationSource(str, Enum):
    current = "current"    # 容器当前（最近一次）退出：state.terminated
    previous = "previous"  # 同一 Pod 的上一次退出：lastState.terminated


class LogsStatus(str, Enum):
    attached = "attached"          # 已确认日志属于该次退出并附上
    unavailable = "unavailable"    # 退出信息保留，但日志拿不到（原因见 logs_reason）
    not_applicable = "not_applicable"  # 没有可见失败，不涉及日志


class LastFailureResponse(BaseModel):
    """补跑 Job 最近一次可见非零退出的诊断结果（只读）。"""

    status: LastFailureStatus
    namespace: str
    job_name: str
    job_uid: str
    pod: str | None = Field(None, description="发生退出的 Pod 名")
    container: str | None = Field(None, description="发生退出的容器名")
    exit_code: int | None = Field(None, description="非零退出码")
    finished_at: datetime | None = Field(None, description="该次退出的结束时间（UTC）")
    termination_source: TerminationSource | None = Field(
        None, description="current=容器当前退出；previous=同一 Pod 的上一次退出"
    )
    restart_count: int | None = Field(None, description="该容器当前累计重启次数")
    logs: str | None = Field(
        None, description="该次退出容器的最后 80 行日志；仅确认属于该次退出才附上"
    )
    logs_status: LogsStatus
    logs_reason: str | None = Field(
        None, description="日志未附上的原因（容器再重启/Pod 被清理/日志接口报错等）"
    )
    checked_pods: int = Field(..., description="纳入检查的单容器 Pod 数")
    skipped_pods: list[str] = Field(
        default_factory=list, description="跳过的多容器 Pod 名（本接口只诊断单容器 Pod）"
    )
    message: str | None = Field(None, description="人类可读提示（如没有可见失败时）")
