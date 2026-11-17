import sqlite3
import threading
from contextlib import contextmanager

# 域级排他锁：两名检修（并发写同一主题域策略）在应用层串行化。
# 落库层另有部分唯一索引兜底，即使绕过锁也会被数据库拦下，双版无法成功落下。
_DOMAIN_LOCKS: dict[str, threading.RLock] = {}
_DOMAIN_LOCKS_GUARD = threading.Lock()


class PolicyError(ValueError):
    """策略写入冲突（版本已存在 / 目标版本不存在 / 双现行被唯一约束拦截）。"""


def _domain_lock(domain_code: str) -> threading.RLock:
    with _DOMAIN_LOCKS_GUARD:
        lock = _DOMAIN_LOCKS.get(domain_code)
        if lock is None:
            lock = threading.RLock()
            _DOMAIN_LOCKS[domain_code] = lock
        return lock


@contextmanager
def domain_write_lock(domain_code: str):
    """主题域排他写锁：同一域的策略写入全程互斥。"""
    lock = _domain_lock(domain_code)
    lock.acquire()
    try:
        yield
    finally:
        lock.release()


_POLICY_COLUMNS = (
    "id, domain_code, domain_name, keep_days, action_on_expire, "
    "version, active, created_at"
)


def list_policies(conn: sqlite3.Connection, active_only: bool = False) -> list[dict]:
    sql = f"SELECT {_POLICY_COLUMNS} FROM retain_policies"
    if active_only:
        sql += " WHERE active = 1"
    sql += " ORDER BY domain_code, version DESC"
    return [dict(r) for r in conn.execute(sql).fetchall()]


def get_policy(conn: sqlite3.Connection, policy_id: int) -> dict | None:
    row = conn.execute(
        f"SELECT {_POLICY_COLUMNS} FROM retain_policies WHERE id = ?",
        (policy_id,),
    ).fetchone()
    return dict(row) if row else None


def get_active_policy(conn: sqlite3.Connection, domain_code: str) -> dict | None:
    """取主题域唯一现行版。

    现行唯一性由 ux_retain_policies_one_active 部分唯一索引保证；
    若库中仍查出多条 active=1（唯一约束被人为关闭等异常），直接抛错，
    绝不静默挑一版——扫描队列因此不会再随机吃版。
    """
    rows = conn.execute(
        f"SELECT {_POLICY_COLUMNS} FROM retain_policies "
        "WHERE domain_code = ? AND active = 1 ORDER BY version DESC",
        (domain_code,),
    ).fetchall()
    if not rows:
        return None
    if len(rows) > 1:
        raise PolicyError(
            f"主题域 {domain_code} 存在 {len(rows)} 条现行策略，"
            "违反现行版唯一约束，已拒绝扫描以避免随机读版"
        )
    return dict(rows[0])


def _begin_immediate_if_needed(conn: sqlite3.Connection) -> bool:
    """开启 IMMEDIATE 事务抢占写锁；若调用方已在事务中则复用。"""
    if conn.in_transaction:
        return False
    conn.execute("BEGIN IMMEDIATE")
    return True


def create_shadow_policy(
    conn: sqlite3.Connection,
    domain_code: str,
    domain_name: str,
    keep_days: int,
    action_on_expire: str,
    version: int | None = None,
) -> dict:
    """落一条影子快照（active=0）。不触碰现行版，任何时刻都只有一条现行。

    version 缺省取该域现有最大版本号 + 1；显式版本号撞车则报错拦截。
    """
    domain_code = domain_code.strip()
    if not domain_code:
        raise PolicyError("domain_code 不能为空")
    if int(keep_days) < 0:
        raise PolicyError("keep_days 不能为负")
    if action_on_expire not in ("归档", "销毁"):
        raise PolicyError("action_on_expire 仅支持 归档 / 销毁")

    with domain_write_lock(domain_code):
        own = _begin_immediate_if_needed(conn)
        try:
            if version is None:
                row = conn.execute(
                    "SELECT MAX(version) AS v FROM retain_policies WHERE domain_code = ?",
                    (domain_code,),
                ).fetchone()
                version = int(row["v"] or 0) + 1
            else:
                version = int(version)
                exists = conn.execute(
                    "SELECT 1 FROM retain_policies WHERE domain_code = ? AND version = ?",
                    (domain_code, version),
                ).fetchone()
                if exists:
                    raise PolicyError(
                        f"主题域 {domain_code} 已存在 v{version}，版本号不可重复"
                    )
            cur = conn.execute(
                "INSERT INTO retain_policies "
                "(domain_code, domain_name, keep_days, action_on_expire, version, active) "
                "VALUES (?,?,?,?,?,0)",
                (domain_code, domain_name, int(keep_days), action_on_expire, version),
            )
            if own:
                conn.commit()
            return get_policy(conn, int(cur.lastrowid))
        except sqlite3.IntegrityError as exc:
            if own:
                conn.rollback()
            raise PolicyError(f"影子策略落库被唯一约束拦截：{exc}") from exc
        except Exception:
            if own:
                conn.rollback()
            raise


def promote_policy(conn: sqlite3.Connection, policy_id: int) -> dict:
    """原子切换现行版：旧现行降为影子(active=0)，目标影子晋升为现行(active=1)。

    全程持有域级排他锁并在单事务内完成；部分唯一索引保证并发晋升时
    只有一个事务能成功，另一个收到 IntegrityError → 409，双版即拦。
    """
    target = get_policy(conn, policy_id)
    if target is None:
        raise PolicyError(f"策略 id={policy_id} 不存在")
    domain_code = target["domain_code"]

    with domain_write_lock(domain_code):
        own = not conn.in_transaction
        if own:
            # IMMEDIATE 立即抢占 SQLite 写锁：并发的第二名检修在事务起点
            # 即被挡住（busy_timeout 后 OperationalError），不会双双降级再晋升。
            conn.execute("BEGIN IMMEDIATE")
        try:
            if target["active"] == 1:
                # 已是现行：幂等返回，不制造多余写入
                if own:
                    conn.commit()
                return target
            conn.execute(
                "UPDATE retain_policies SET active = 0 "
                "WHERE domain_code = ? AND active = 1",
                (domain_code,),
            )
            try:
                conn.execute(
                    "UPDATE retain_policies SET active = 1 WHERE id = ?",
                    (policy_id,),
                )
            except sqlite3.IntegrityError as exc:
                raise PolicyError(
                    "现行版切换冲突：另一检修已晋升该域策略，本次操作被唯一约束拦截"
                ) from exc
            refreshed = get_policy(conn, policy_id)
            # 落库后再核一次：该域有且仅有一条现行
            count = conn.execute(
                "SELECT COUNT(*) AS c FROM retain_policies "
                "WHERE domain_code = ? AND active = 1",
                (domain_code,),
            ).fetchone()["c"]
            if count != 1:
                raise PolicyError(
                    f"晋升后主题域 {domain_code} 现行版数量为 {count}，预期 1"
                )
            if own:
                conn.commit()
            return refreshed
        except Exception:
            if own:
                conn.rollback()
            raise


def publish_policy(
    conn: sqlite3.Connection,
    domain_code: str,
    domain_name: str,
    keep_days: int,
    action_on_expire: str,
    version: int | None = None,
) -> dict:
    """一步到位：先落影子快照，再在同一域锁/事务内原子晋升为现行。"""
    domain_code = domain_code.strip()
    with domain_write_lock(domain_code):
        own = _begin_immediate_if_needed(conn)
        try:
            shadow = create_shadow_policy(
                conn,
                domain_code,
                domain_name,
                keep_days,
                action_on_expire,
                version,
            )
            promoted = promote_policy(conn, shadow["id"])
            if own:
                conn.commit()
            return promoted
        except Exception:
            if own:
                conn.rollback()
            raise
