"""失败诊断接口测试：最近一次非零退出、日志归属复核、各类故障分支。

种子数据（mock_k8s.server.seed_state）：
- inspection-daily-catchup-202611190100：p1 上一次退出(01:05Z, exit 1)、
  p2 当前退出(01:03Z, exit 2)、multi 多容器（应跳过）；
- inspection-daily-catchup-flaky：取日志途中容器再次重启；
- inspection-daily-catchup-202611170100：全部成功。
"""
import pytest

DEMO_JOB = "inspection-daily-catchup-202611190100"
FLAKY_JOB = "inspection-daily-catchup-flaky"
OK_JOB = "inspection-daily-catchup-202611170100"


def _uid(harness, job_name: str) -> str:
    return harness["state"]["jobs"][("default", job_name)]["metadata"]["uid"]


async def _call(harness, job_name: str, uid: str):
    return await harness["api"].get(
        f"/namespaces/default/jobs/{job_name}/last-failure",
        params={"uid": uid},
    )


async def test_picks_latest_previous_termination_with_logs(harness):
    """最近一次失败是 p1 的上一次退出（01:05Z 晚于 p2 的 01:03Z），日志附上一致。"""
    resp = await _call(harness, DEMO_JOB, _uid(harness, DEMO_JOB))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "found"
    assert body["pod"] == f"{DEMO_JOB}-p1"
    assert body["container"] == "worker"
    assert body["exit_code"] == 1
    assert body["finished_at"].endswith("01:05:00Z")
    assert body["termination_source"] == "previous"
    assert body["restart_count"] == 2
    # 日志确认属于上一次退出的容器实例 c-p1-run2，且按 80 行截断
    assert body["logs_status"] == "attached"
    assert body["logs_reason"] is None
    logs = body["logs"]
    assert logs is not None
    lines = logs.splitlines()
    assert len(lines) == 80
    assert "c-p1-run2" in logs
    assert "disk latency 990ms" in lines[-1]
    # 多容器 Pod 被跳过
    assert body["checked_pods"] == 2
    assert body["skipped_pods"] == [f"{DEMO_JOB}-multi"]


async def test_restart_during_log_fetch_keeps_exit_without_logs(harness):
    """取日志途中容器再次重启：保留退出信息，不附日志并说明原因。"""
    resp = await _call(harness, FLAKY_JOB, _uid(harness, FLAKY_JOB))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "found"
    assert body["pod"] == f"{FLAKY_JOB}-p1"
    assert body["exit_code"] == 1
    assert body["termination_source"] == "current"
    assert body["logs"] is None
    assert body["logs_status"] == "unavailable"
    assert "重启" in body["logs_reason"]

    # 再调一次：上次退出已落入 lastState，这次能确认归属并附上日志
    resp2 = await _call(harness, FLAKY_JOB, _uid(harness, FLAKY_JOB))
    body2 = resp2.json()
    assert body2["status"] == "found"
    assert body2["termination_source"] == "previous"
    assert body2["logs_status"] == "attached"
    assert body2["logs"] is not None
    assert "c-flaky-run1" in body2["logs"]


@pytest.mark.mock_pod_fault("pod-gone-on-log")
async def test_pod_gone_during_log_fetch(harness):
    """取日志时 Pod 已被清理：保留退出信息，不附日志。"""
    resp = await _call(harness, DEMO_JOB, _uid(harness, DEMO_JOB))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "found"
    assert body["exit_code"] == 1
    assert body["logs"] is None
    assert body["logs_status"] == "unavailable"
    assert "清理" in body["logs_reason"]


@pytest.mark.mock_pod_fault("log-error")
async def test_log_api_error_keeps_exit_without_logs(harness):
    """日志接口报错：保留退出信息，不附日志。"""
    resp = await _call(harness, DEMO_JOB, _uid(harness, DEMO_JOB))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "found"
    assert body["logs"] is None
    assert body["logs_status"] == "unavailable"
    assert "日志接口报错" in body["logs_reason"]


async def test_no_visible_failure(harness):
    """全部成功的 Job：明确提示没有可见失败。"""
    resp = await _call(harness, OK_JOB, _uid(harness, OK_JOB))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "no_visible_failure"
    assert body["pod"] is None
    assert body["exit_code"] is None
    assert body["logs"] is None
    assert body["logs_status"] == "not_applicable"
    assert "没有可见的非零退出" in body["message"]


@pytest.mark.mock_pod_fault("pods-list-error")
async def test_pod_list_failure_is_distinct(harness):
    """Pod 列表查不到：与“没有可见失败”分开提示（502）。"""
    resp = await _call(harness, DEMO_JOB, _uid(harness, DEMO_JOB))
    assert resp.status_code == 502
    assert "Pod 列表" in resp.json()["detail"]


async def test_job_not_found(harness):
    resp = await _call(harness, "no-such-job", "uid-whatever")
    assert resp.status_code == 404
    assert "不存在" in resp.json()["detail"]


async def test_uid_mismatch_rejected(harness):
    resp = await _call(harness, DEMO_JOB, "uid-not-match")
    assert resp.status_code == 409
    assert "不一致" in resp.json()["detail"]
