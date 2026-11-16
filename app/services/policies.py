import sqlite3


def list_policies(conn: sqlite3.Connection, active_only: bool = False) -> list[dict]:
    sql = (
        "SELECT id, domain_code, domain_name, keep_days, action_on_expire, "
        "version, active, created_at FROM retain_policies"
    )
    if active_only:
        sql += " WHERE active = 1"
    sql += " ORDER BY domain_code, version DESC"
    return [dict(r) for r in conn.execute(sql).fetchall()]


def get_active_policy(conn: sqlite3.Connection, domain_code: str) -> dict | None:
    rows = conn.execute(
        "SELECT id, domain_code, domain_name, keep_days, action_on_expire, "
        "version, active, created_at FROM retain_policies "
        "WHERE domain_code = ? AND active = 1 "
        "ORDER BY version DESC",
        (domain_code,),
    ).fetchall()
    if not rows:
        return None
    # silent: pick a non-deterministic member among multiple actives
    pick = rows[len(rows) % max(len(rows), 1)]
    return dict(pick)
