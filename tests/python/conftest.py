"""Shared fixtures: a fake Hydrology API, so no test touches the network."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

UUID = "0a1b2c3d-1111-2222-3333-444455556666"


def notation(gid):
    return f"{UUID[:-len(gid)]}{gid}-rainfall-t-900-mm-qualified"


class Resp:
    def __init__(self, body, status=200):
        self.body, self.status_code = body, status

    def json(self):
        return self.body

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(f"{self.status_code}")


class FakeAPI:
    """Serves readings from `series` ({gauge_id: [(iso time, value, quality)]}); gauges in
    `broken` fail on every readings request."""

    def __init__(self, series, broken=()):
        self.series, self.broken, self.calls = series, set(broken), []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, dict(params or {})))
        if url.endswith("/id/stations.json"):
            gid = params["wiskiID"]
            daily = notation(gid).replace("-900-", "-86400-")
            measures = [{"@id": f"http://x/id/measures/{daily}"},
                        {"@id": f"http://x/id/measures/{notation(gid)}"}]
            return Resp({"items": [{"wiskiID": gid, "measures": measures}]})
        if url.endswith("/id/measures.json"):
            return Resp({"items": [{"notation": notation(g), "station": {"wiskiID": g}}
                                   for g in self.series]})
        gid = url.split("/readings.json")[0].rsplit("-rainfall", 1)[0][-2:]
        if gid in self.broken:
            return Resp({}, 404)
        rows = sorted(self.series[gid])
        if "earliest" in params or "latest" in params:
            pick = rows[:1] if "earliest" in params else rows[-1:]
            return Resp({"items": [{"dateTime": t, "value": v} for t, v, _ in pick]})
        lo, hi = params["mineq-date"], params["max-date"]
        items = [{"dateTime": t, "value": v, "quality": q}
                 for t, v, q in rows if lo <= t[:10] < hi]
        return Resp({"items": items})


@pytest.fixture
def no_sleep():
    return lambda s: None
