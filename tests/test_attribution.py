"""Tests for station attribution and physical-station daily series queries."""
from __future__ import annotations

import io
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date

from conftest import full_csv


def _upload(client, station, day, raw, expected_version=0):
    return client.post(
        f"/stations/{station}/dates/{day}/uploads",
        data={"expected_version": str(expected_version)},
        files={"file": ("data.csv", io.BytesIO(raw), "text/csv")},
    )


def _register(client, physical, source, start, end):
    return client.post(
        "/attributions",
        json={
            "physical_station_id": physical,
            "source_station_id": source,
            "start_date": start,
            "end_date": end,
        },
    )


def test_register_attribution_and_query(client):
    resp = _register(client, "PHYS-1", "OLD-1", "2026-11-01", "2026-11-10")
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["physical_station_id"] == "PHYS-1"
    assert body["source_station_id"] == "OLD-1"
    assert body["start_date"] == "2026-11-01"
    assert body["end_date"] == "2026-11-10"
    assert body["id"] > 0

    listing = client.get("/attributions").json()
    assert listing["count"] == 1
    assert listing["attributions"][0]["id"] == body["id"]

    by_physical = client.get(
        "/attributions", params={"physical_station_id": "PHYS-1"}
    ).json()
    assert by_physical["count"] == 1
    by_other = client.get(
        "/attributions", params={"physical_station_id": "PHYS-2"}
    ).json()
    assert by_other["count"] == 0


def test_exact_duplicate_registration_is_idempotent(client):
    first = _register(client, "PHYS-1", "OLD-1", "2026-11-01", "2026-11-10")
    assert first.status_code == 201
    again = _register(client, "PHYS-1", "OLD-1", "2026-11-01", "2026-11-10")
    assert again.status_code == 200
    assert again.json()["id"] == first.json()["id"]
    assert client.get("/attributions").json()["count"] == 1


def test_same_source_same_day_cannot_belong_to_two_physical_stations(client):
    r1 = _register(client, "PHYS-1", "SRC-1", "2026-11-01", "2026-11-10")
    assert r1.status_code == 201

    # 11-05 is covered by both ranges -> conflict, nothing registered.
    r2 = _register(client, "PHYS-2", "SRC-1", "2026-11-05", "2026-11-20")
    assert r2.status_code == 409
    body = r2.json()
    assert body["error"] == "attribution_conflict"
    assert body["conflicts"][0]["physical_station_id"] == "PHYS-1"
    assert body["conflicts"][0]["overlap_start"] == "2026-11-05"
    assert body["conflicts"][0]["overlap_end"] == "2026-11-10"

    assert client.get("/attributions").json()["count"] == 1


def test_adjacent_ranges_for_different_physical_stations_are_fine(client):
    assert _register(client, "PHYS-1", "SRC-1", "2026-11-01", "2026-11-10").status_code == 201
    assert _register(client, "PHYS-2", "SRC-1", "2026-11-11", "2026-11-20").status_code == 201
    assert client.get("/attributions").json()["count"] == 2


def test_same_physical_station_may_have_overlapping_source_ids(client):
    # Hand-over period: old and new number both map to the same station.
    assert _register(client, "PHYS-1", "OLD-1", "2026-11-01", "2026-11-10").status_code == 201
    assert _register(client, "PHYS-1", "NEW-1", "2026-11-05", "2026-11-20").status_code == 201


def test_invalid_registration_inputs_rejected(client):
    assert _register(
        client, "PHYS-1", "SRC-1", "2026-11-10", "2026-11-01"
    ).status_code == 400
    assert _register(
        client, "bad physical!", "SRC-1", "2026-11-01", "2026-11-10"
    ).status_code == 400
    assert client.post(
        "/attributions",
        json={
            "physical_station_id": "PHYS-1",
            "source_station_id": "SRC-1",
            "start_date": "not-a-date",
            "end_date": "2026-11-10",
        },
    ).status_code == 422


def test_concurrent_registration_same_source_different_stations_one_wins(client):
    outcomes: list[int] = []
    lock = threading.Lock()

    def register(i: int):
        resp = _register(
            client, f"PHYS-{i}", "SRC-1", "2026-11-01", "2026-11-10"
        )
        with lock:
            outcomes.append(resp.status_code)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(register, range(8)))

    assert outcomes.count(201) == 1
    assert outcomes.count(409) == 7
    rows = client.get(
        "/attributions", params={"source_station_id": "SRC-1"}
    ).json()["attributions"]
    assert len(rows) == 1
    assert rows[0]["start_date"] == "2026-11-01"


def test_concurrent_registration_different_sources_both_succeed(client):
    outcomes: list[int] = []
    lock = threading.Lock()

    def register(i: int):
        resp = _register(client, "PHYS-1", f"SRC-{i}", "2026-11-01", "2026-11-10")
        with lock:
            outcomes.append(resp.status_code)

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(register, range(4)))

    assert outcomes.count(201) == 4


def test_series_stitches_old_and_new_source_numbers(client):
    # Old number covers 11-01..11-10, new number covers 11-11..11-20.
    _register(client, "PHYS-1", "OLD-1", "2026-11-01", "2026-11-10")
    _register(client, "PHYS-1", "NEW-1", "2026-11-11", "2026-11-20")
    _upload(client, "OLD-1", "2026-11-01", full_csv({h: 1.0 for h in range(24)}))
    _upload(client, "NEW-1", "2026-11-01", full_csv({h: 2.0 for h in range(24)}))
    _upload(client, "OLD-1", "2026-11-10", full_csv({h: 3.0 for h in range(24)}))
    _upload(client, "NEW-1", "2026-11-11", full_csv({h: 4.0 for h in range(24)}))

    resp = client.get(
        "/physical-stations/PHYS-1/daily-series",
        params={"start_date": "2026-11-01", "end_date": "2026-11-12"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["day_count"] == 12
    days = {d["data_date"]: d for d in body["days"]}

    assert days["2026-11-01"]["status"] == "ok"
    assert days["2026-11-01"]["source_station_id"] == "OLD-1"
    assert days["2026-11-01"]["version"] == 1
    assert days["2026-11-01"]["records"][0] == {"hour": 0, "pm25": 1.0}
    assert len(days["2026-11-01"]["records"]) == 24

    # New number's 11-01 data belongs to a different physical station day;
    # PHYS-1 on 11-01 must still use the old number.
    assert days["2026-11-10"]["source_station_id"] == "OLD-1"
    assert days["2026-11-10"]["records"][0]["pm25"] == 3.0
    assert days["2026-11-11"]["source_station_id"] == "NEW-1"
    assert days["2026-11-11"]["records"][0]["pm25"] == 4.0

    # Days without published data are reported as missing, not dropped.
    assert days["2026-11-02"]["status"] == "missing"
    assert days["2026-11-12"]["status"] == "missing"
    assert body["missing_count"] == 9  # 11-02..11-09 and 11-12


def test_series_reports_backfilled_current_version(client):
    _register(client, "PHYS-1", "SRC-1", "2026-11-01", "2026-11-02")
    _upload(client, "SRC-1", "2026-11-01", full_csv({h: 1.0 for h in range(24)}))
    _upload(
        client, "SRC-1", "2026-11-01",
        full_csv({h: 9.0 for h in range(24)}), expected_version=1,
    )

    body = client.get(
        "/physical-stations/PHYS-1/daily-series",
        params={"start_date": "2026-11-01", "end_date": "2026-11-01"},
    ).json()
    day = body["days"][0]
    assert day["version"] == 2
    assert day["records"][0]["pm25"] == 9.0


def test_series_reports_source_conflict_instead_of_picking_one(client):
    # Overlapping attribution for the same physical station is allowed; when
    # both sources actually published the same day the answer is ambiguous.
    _register(client, "PHYS-1", "OLD-1", "2026-11-01", "2026-11-10")
    _register(client, "PHYS-1", "NEW-1", "2026-11-05", "2026-11-20")
    _upload(client, "OLD-1", "2026-11-05", full_csv({h: 1.0 for h in range(24)}))
    _upload(client, "NEW-1", "2026-11-05", full_csv({h: 2.0 for h in range(24)}))
    _upload(client, "NEW-1", "2026-11-06", full_csv({h: 3.0 for h in range(24)}))

    body = client.get(
        "/physical-stations/PHYS-1/daily-series",
        params={"start_date": "2026-11-05", "end_date": "2026-11-06"},
    ).json()
    conflict = body["days"][0]
    assert conflict["data_date"] == "2026-11-05"
    assert conflict["status"] == "source_conflict"
    assert conflict["records"] is None
    assert {s["source_station_id"] for s in conflict["sources"]} == {"OLD-1", "NEW-1"}
    assert body["conflict_count"] == 1

    # 11-06 is unambiguous: only NEW-1 has data.
    assert body["days"][1]["status"] == "ok"
    assert body["days"][1]["source_station_id"] == "NEW-1"


def test_series_without_registration_reports_every_day_missing(client):
    _upload(client, "SRC-1", "2026-11-01", full_csv())
    body = client.get(
        "/physical-stations/PHYS-UNKNOWN/daily-series",
        params={"start_date": "2026-11-01", "end_date": "2026-11-03"},
    ).json()
    assert body["day_count"] == 3
    assert all(d["status"] == "missing" for d in body["days"])
    assert body["missing_count"] == 3


def test_series_validates_range_and_station(client):
    bad = client.get(
        "/physical-stations/PHYS-1/daily-series",
        params={"start_date": "2026-11-10", "end_date": "2026-11-01"},
    )
    assert bad.status_code == 400
    bad_id = client.get(
        "/physical-stations/bad physical!/daily-series",
        params={"start_date": "2026-11-01", "end_date": "2026-11-02"},
    )
    assert bad_id.status_code == 400
    assert client.get(
        "/physical-stations/PHYS-1/daily-series",
        params={"start_date": "2026-11-01"},
    ).status_code == 422


def test_series_uses_one_snapshot_when_attribution_changes_mid_query(client):
    """A registration committed mid-query must not leak into that query.

    The db seam fires after the attribution rows are read; the concurrent
    registration (in another connection) commits before records are read.
    The query must still resolve days using the snapshot it started with.
    """
    from app import db as db_module

    _register(client, "PHYS-1", "OLD-1", "2026-11-01", "2026-11-02")
    _upload(client, "OLD-1", "2026-11-02", full_csv({h: 1.0 for h in range(24)}))

    def concurrent_write():
        _register(client, "PHYS-1", "NEW-1", "2026-11-02", "2026-11-02")
        _upload(client, "NEW-1", "2026-11-02", full_csv({h: 2.0 for h in range(24)}))

    snapshot = db_module.get_physical_station_series(
        "PHYS-1", date(2026, 11, 2), date(2026, 11, 2),
        _after_attributions=concurrent_write,
    )
    assert snapshot["days"][0]["status"] == "ok"
    assert snapshot["days"][0]["source_station_id"] == "OLD-1"
    assert snapshot["days"][0]["records"][0]["pm25"] == 1.0

    # A new query sees the committed registration and reports the ambiguity.
    after = client.get(
        "/physical-stations/PHYS-1/daily-series",
        params={"start_date": "2026-11-02", "end_date": "2026-11-02"},
    ).json()
    assert after["days"][0]["status"] == "source_conflict"


def test_concurrent_series_queries_during_backfill_never_error(client):
    _register(client, "PHYS-1", "SRC-1", "2026-11-01", "2026-11-01")
    _upload(client, "SRC-1", "2026-11-01", full_csv({h: 1.0 for h in range(24)}))

    results: list[int] = []
    lock = threading.Lock()

    def query():
        resp = client.get(
            "/physical-stations/PHYS-1/daily-series",
            params={"start_date": "2026-11-01", "end_date": "2026-11-01"},
        )
        with lock:
            results.append(resp.status_code)
        if resp.status_code == 200:
            day = resp.json()["days"][0]
            assert len(day["records"]) == 24

    def backfill():
        resp = _upload(
            client, "SRC-1", "2026-11-01",
            full_csv({h: 5.0 for h in range(24)}), expected_version=1,
        )
        assert resp.status_code in (201, 409)

    with ThreadPoolExecutor(max_workers=9) as pool:
        futures = [pool.submit(query) for _ in range(8)]
        futures.append(pool.submit(backfill))
        for f in futures:
            f.result()

    assert results == [200] * 8
