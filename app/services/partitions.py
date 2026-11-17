import sqlite3

_PARTITION_COLUMNS = (
    "p.id AS id, p.domain_code AS domain_code, p.biz_date AS biz_date, "
    "p.row_count AS row_count, p.storage_state AS storage_state, "
    "p.archive_uri AS archive_uri, p.created_at AS created_at, "
    "rp.id AS current_policy_id, rp.version AS current_policy_version, "
    "rp.keep_days AS current_keep_days, rp.action_on_expire AS current_action"
)

# 分区与该域唯一现行版 LEFT JOIN：现行版唯一约束保证每域至多一条，
# 不会放大行数；无现行版时策略字段为 None，页面显式标「未挂现行策略」。
_PARTITION_FROM = (
    "FROM partitions p "
    "LEFT JOIN retain_policies rp "
    "ON rp.domain_code = p.domain_code AND rp.active = 1"
)


def list_partitions(
    conn: sqlite3.Connection,
    domain_code: str | None = None,
    storage_state: str | None = None,
) -> list[dict]:
    clauses: list[str] = []
    params: list[object] = []
    if domain_code:
        clauses.append("p.domain_code = ?")
        params.append(domain_code)
    if storage_state:
        clauses.append("p.storage_state = ?")
        params.append(storage_state)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    rows = conn.execute(
        f"SELECT {_PARTITION_COLUMNS} {_PARTITION_FROM}{where} "
        "ORDER BY p.domain_code, p.biz_date",
        params,
    ).fetchall()
    return [dict(r) for r in rows]


def get_partition(conn: sqlite3.Connection, partition_id: int) -> dict | None:
    row = conn.execute(
        f"SELECT {_PARTITION_COLUMNS} {_PARTITION_FROM} WHERE p.id = ?",
        (partition_id,),
    ).fetchone()
    return dict(row) if row else None
