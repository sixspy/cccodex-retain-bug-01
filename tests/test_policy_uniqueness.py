"""现行版唯一性 / 影子晋升原子切换 / 排他锁双写拦截 / 扫描读版确定性。

覆盖验收口径：
- 现行标记用唯一约束落库，双版即拦（直接写库 + API 双通道）；
- 影子快照晋升为原子切换，全程每域恰好一条现行；
- 两名检修并发写同域，不加锁会双成功——这里必须被锁 + 唯一约束挡住；
- 扫描只读唯一现行版，不随机吃版；分区卡片与现行策略对齐。
"""
import sqlite3
import threading

import pytest

from app.db import get_db, init_db
from app.services.policies import (
    PolicyError,
    create_shadow_policy,
    get_active_policy,
    promote_policy,
    publish_policy,
)
from app.services.partitions import list_partitions
from app.services.scan import run_expire_scan


@pytest.fixture()
def db_path(tmp_path):
    path = str(tmp_path / "uniq.db")
    init_db(path)
    return path


def _active_rows(conn, domain):
    return conn.execute(
        "SELECT * FROM retain_policies WHERE domain_code = ? AND active = 1",
        (domain,),
    ).fetchall()


def test_seed_has_exactly_one_active_per_domain(db_path):
    with get_db(db_path) as conn:
        rows = conn.execute(
            "SELECT domain_code, COUNT(*) AS c FROM retain_policies "
            "WHERE active = 1 GROUP BY domain_code"
        ).fetchall()
        assert rows, "种子应有现行策略"
        assert all(r["c"] == 1 for r in rows)


def test_direct_dual_active_insert_is_blocked_by_unique_index(db_path):
    with get_db(db_path) as conn:
        # 已存在 orders 现行版，再强插一条 active=1 必须被部分唯一索引拦下
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO retain_policies "
                "(domain_code, domain_name, keep_days, action_on_expire, version, active) "
                "VALUES ('orders','订单明细域',30,'归档',99,1)"
            )
        conn.rollback()
        assert len(_active_rows(conn, "orders")) == 1


def test_shadow_insert_does_not_touch_active(db_path):
    with get_db(db_path) as conn:
        before = get_active_policy(conn, "orders")
        shadow = create_shadow_policy(conn, "orders", "订单明细域", 11, "销毁")
        assert shadow["active"] == 0
        after = get_active_policy(conn, "orders")
        assert after["id"] == before["id"]
        assert len(_active_rows(conn, "orders")) == 1


def test_duplicate_shadow_version_rejected(db_path):
    with get_db(db_path) as conn:
        with pytest.raises(PolicyError):
            create_shadow_policy(
                conn, "orders", "订单明细域", 11, "销毁", version=2
            )


def test_promote_is_atomic_switch(db_path):
    with get_db(db_path) as conn:
        shadow = create_shadow_policy(conn, "orders", "订单明细域", 12, "销毁")
        old = get_active_policy(conn, "orders")
        assert old["version"] == 1

        promoted = promote_policy(conn, shadow["id"])
        assert promoted["active"] == 1
        assert promoted["version"] == shadow["version"]

        actives = _active_rows(conn, "orders")
        assert len(actives) == 1
        assert actives[0]["id"] == shadow["id"]
        # 旧现行已降为影子
        old_row = conn.execute(
            "SELECT active FROM retain_policies WHERE id = ?", (old["id"],)
        ).fetchone()
        assert old_row["active"] == 0
        # 读取口径确定性地指向新现行
        assert get_active_policy(conn, "orders")["id"] == shadow["id"]


def test_publish_creates_then_promotes_in_one_lock(db_path):
    with get_db(db_path) as conn:
        policy = publish_policy(conn, "orders", "订单明细域", 21, "归档")
        assert policy["active"] == 1
        assert len(_active_rows(conn, "orders")) == 1
        assert get_active_policy(conn, "orders")["keep_days"] == 21


def test_domain_lock_is_exclusive(db_path):
    """域级排他锁：持锁期间另一线程无法进入同域写临界区。"""
    from app.services.policies import _domain_lock

    lock = _domain_lock("orders")
    blocked = []
    lock.acquire()

    def try_enter():
        got = lock.acquire(timeout=0.5)
        if got:
            lock.release()
        blocked.append(not got)

    t = threading.Thread(target=try_enter)
    t.start()
    t.join()
    lock.release()
    assert blocked == [True]


def test_sqlite_write_lock_blocks_second_immediate_tx(db_path):
    """数据库层兜底：两条独立连接的 IMMEDIATE 写事务互斥，后到者失败。"""
    from app.db import connect

    c1 = connect(db_path)
    c2 = connect(db_path)
    c1.execute("PRAGMA busy_timeout = 1000")
    c2.execute("PRAGMA busy_timeout = 1000")
    c1.execute("BEGIN IMMEDIATE")
    with pytest.raises(sqlite3.OperationalError):
        c2.execute("BEGIN IMMEDIATE")
    c1.rollback()
    c1.close()
    c2.close()


def test_concurrent_promotions_keep_single_active(db_path):
    """两名检修并发晋升同域：域锁串行化 + 唯一索引兜底，任何时刻无双现行。"""
    with get_db(db_path) as conn:
        s1 = create_shadow_policy(conn, "orders", "订单明细域", 31, "归档")
        s2 = create_shadow_policy(conn, "orders", "订单明细域", 32, "销毁")
        ids = (s1["id"], s2["id"])

    outcomes: list[bool] = []
    barrier = threading.Barrier(2)

    def worker(pid):
        barrier.wait()
        try:
            with get_db(db_path) as conn:
                promote_policy(conn, pid)
            outcomes.append(True)
        except (PolicyError, sqlite3.IntegrityError, sqlite3.OperationalError):
            outcomes.append(False)

    t1 = threading.Thread(target=worker, args=(ids[0],))
    t2 = threading.Thread(target=worker, args=(ids[1],))
    t1.start(); t2.start()
    t1.join(timeout=30); t2.join(timeout=30)

    assert len(outcomes) == 2
    with get_db(db_path) as conn:
        actives = _active_rows(conn, "orders")
        assert len(actives) == 1, "并发写后同域只能有一条现行版，双版不得落下"


def test_get_active_policy_raises_instead_of_silent_pick(db_path):
    """绕过应用层制造双现行后，读版必须显式报错，不能静默随机挑版。"""
    with get_db(db_path) as conn:
        conn.execute("DROP INDEX ux_retain_policies_one_active")
        conn.execute(
            "INSERT INTO retain_policies "
            "(domain_code, domain_name, keep_days, action_on_expire, version, active) "
            "VALUES ('orders','订单明细域',40,'归档',88,1)"
        )
        conn.commit()
        with pytest.raises(PolicyError):
            get_active_policy(conn, "orders")


def test_scan_always_uses_unique_active_version(db_path):
    with get_db(db_path) as conn:
        # orders 现行 v1 keep_days=7；发布 v3 keep_days=1 后扫描应稳定吃 v3
        publish_policy(conn, "orders", "订单明细域", 1, "归档")
        job = run_expire_scan(conn, "2026-11-16")
        assert job["status"] == "已完成"
        rows = conn.execute(
            "SELECT DISTINCT policy_version, keep_days FROM judgments "
            "WHERE domain_code = 'orders'"
        ).fetchall()
        assert len(rows) == 1, "同一域扫描只能读到一个现行版本"
        assert rows[0]["policy_version"] == 3
        assert rows[0]["keep_days"] == 1


def test_partition_cards_align_with_active_policy(db_path):
    with get_db(db_path) as conn:
        publish_policy(conn, "orders", "订单明细域", 5, "销毁")
        parts = [p for p in list_partitions(conn) if p["domain_code"] == "orders"]
        assert parts
        active = get_active_policy(conn, "orders")
        for p in parts:
            assert p["current_policy_id"] == active["id"]
            assert p["current_policy_version"] == active["version"]
            assert p["current_keep_days"] == active["keep_days"]
            assert p["current_action"] == "销毁"


def test_api_publish_and_promote_endpoints(authed):
    pub = authed.post(
        "/api/policies/publish",
        json={
            "domain_code": "orders",
            "domain_name": "订单明细域",
            "keep_days": 6,
            "action_on_expire": "归档",
        },
    )
    assert pub.status_code == 200
    policy = pub.json()["policy"]
    assert policy["active"] == 1

    # 再造一条影子，然后走晋升接口
    shadow = authed.post(
        "/api/policies/shadow",
        json={
            "domain_code": "orders",
            "domain_name": "订单明细域",
            "keep_days": 8,
            "action_on_expire": "销毁",
        },
    ).json()["policy"]
    assert shadow["active"] == 0
    promoted = authed.post(f"/api/policies/{shadow['id']}/promote")
    assert promoted.status_code == 200
    assert promoted.json()["policy"]["active"] == 1

    items = authed.get("/api/policies").json()["items"]
    actives = [p for p in items if p["domain_code"] == "orders" and p["active"] == 1]
    assert len(actives) == 1
    assert actives[0]["id"] == shadow["id"]


def test_api_shadow_duplicate_version_returns_409(authed):
    r = authed.post(
        "/api/policies/shadow",
        json={
            "domain_code": "orders",
            "domain_name": "订单明细域",
            "keep_days": 8,
            "action_on_expire": "销毁",
            "version": 2,
        },
    )
    assert r.status_code == 409
