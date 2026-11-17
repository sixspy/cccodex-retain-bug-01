def test_login_page_has_password_hint(client):
    r = client.get("/login")
    assert r.status_code == 200
    text = r.text
    assert "登录" in text or "账号" in text
    assert "密码" in text


def test_login_and_meta(authed):
    r = authed.get("/api/meta")
    assert r.status_code == 200
    body = r.json()
    assert body["lab_code"] == "cc-retain-lab"
    assert "留存" in body["lab_name"]


def test_seed_policies_and_partitions(authed):
    policies = authed.get("/api/policies").json()["items"]
    parts = authed.get("/api/partitions").json()["items"]
    assert len(policies) >= 4
    assert len(parts) >= 10
    # 列表同时返回现行版与影子版；现行版每域有且仅有一条
    active = [p for p in policies if p["active"] == 1]
    domains = {p["domain_code"] for p in active}
    assert len(active) == len(domains)
    # 种子自带 orders 影子版本（v2, active=0），用于演示晋升
    orders_versions = sorted(
        (p["version"], p["active"]) for p in policies if p["domain_code"] == "orders"
    )
    assert (2, 0) in orders_versions
    # 分区卡片必须挂上现行策略版本，与策略列表对齐
    assert all(p.get("current_policy_version") for p in parts)
