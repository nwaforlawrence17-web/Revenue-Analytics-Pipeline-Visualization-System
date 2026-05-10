from __future__ import annotations

import csv
import datetime as dt
import functools
import json
import math
import os
import statistics
import sys
import sqlite3
import pandas as pd
import urllib.parse
from dataclasses import dataclass
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterable, Optional


def _json_dumps(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")


def _read_json_body(handler: SimpleHTTPRequestHandler) -> Any:
    content_length = handler.headers.get("Content-Length")
    if not content_length:
        raise ValueError("Missing Content-Length")
    raw = handler.rfile.read(int(content_length))
    try:
        return json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON: {e.msg}") from e


def _parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    try:
        num = float(text)
    except ValueError:
        return None
    if math.isnan(num) or math.isinf(num):
        return None
    return num


def _parse_bool(value: str | None) -> bool:
    if value is None:
        return False
    text = str(value).strip().lower()
    return text in {"true", "1", "yes", "y", "t"}


def _parse_date(value: str | None) -> dt.date | None:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return dt.datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _date_to_iso(value: dt.date | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _month_bucket(d: dt.date) -> dt.date:
    return dt.date(d.year, d.month, 1)


def _week_bucket(d: dt.date) -> dt.date:
    # ISO week starting Monday
    return d - dt.timedelta(days=d.weekday())


def _safe_div(n: float, d: float) -> float | None:
    if d == 0:
        return None
    return n / d


REGION_CANONICAL_MAP = {
    "NORTH AMERICA": "North America",
    "APAC": "Asia-Pacific",
    "EMEA": "Europe, Middle East and Africa",
    "AFRICA": "Africa",
}


def canonical_region(raw_region: str | None) -> str:
    text = (raw_region or "").strip()
    if text == "":
        return "Unknown"
    upper = text.upper()
    return REGION_CANONICAL_MAP.get(upper, text.title())


def region_group(region_full: str) -> str:
    # Derived only from canonical region (never manually edited)
    if region_full == "North America":
        return "AMERICAS"
    if region_full == "Asia-Pacific":
        return "APAC"
    if region_full in {"Europe, Middle East and Africa", "Africa"}:
        return "EMEA"
    return "OTHER"


@dataclass(frozen=True)
class Record:
    order_id: str
    order_date: dt.date | None
    order_status: str
    customer_id: str
    customer_name: str
    region: str
    region_group: str
    country: str
    revenue_usd: float | None
    cost_usd: float | None
    profit_usd: float | None
    margin_pct: float | None

    # pass-through fields for audit table (not used for KPI math)
    payment_method: str
    category: str
    product: str


def _quantile_box(values: list[float]) -> dict[str, Any] | None:
    clean = [v for v in values if v is not None and not math.isnan(v) and not math.isinf(v)]
    if len(clean) < 3:
        return None
    clean.sort()
    q1, q2, q3 = statistics.quantiles(clean, n=4, method="inclusive")
    iqr = q3 - q1
    low_fence = q1 - 1.5 * iqr
    high_fence = q3 + 1.5 * iqr
    inlier = [v for v in clean if low_fence <= v <= high_fence]
    if not inlier:
        return None
    whisker_low = min(inlier)
    whisker_high = max(inlier)
    outliers = [v for v in clean if v < low_fence or v > high_fence]
    return {
        "count": len(clean),
        "q1": q1,
        "median": q2,
        "q3": q3,
        "iqr": iqr,
        "whisker_low": whisker_low,
        "whisker_high": whisker_high,
        "outliers": outliers[:200],  # cap payload
    }


class SemanticLayer:
    """
    Strict semantic layer over SQL Warehouse (SQLite):
      - Metric business logic is locked in SQL templates
      - Demonstrates high-level SQL proficiency (Aggregations, Parameterization, Performance)
    """

    METRICS: dict[str, dict[str, Any]] = {
        "revenue_total_usd": {
            "name": "Total Revenue (USD)",
            "unit": "USD",
            "type": "currency",
            "definition": "SELECT SUM(revenue_usd) FROM orders WHERE order_status='COMPLETED'",
            "locked_filters": {"order_status": ["COMPLETED"]},
        },
        "profit_total_usd": {
            "name": "Net Profit (USD)",
            "unit": "USD",
            "type": "currency",
            "definition": "SELECT SUM(profit_usd) FROM orders WHERE order_status='COMPLETED'",
            "locked_filters": {"order_status": ["COMPLETED"]},
        },
        "profit_margin_pct": {
            "name": "Profit Margin (%)",
            "unit": "PCT",
            "type": "percentage",
            "definition": "SELECT (SUM(profit_usd) / SUM(revenue_usd)) * 100 FROM orders WHERE order_status='COMPLETED'",
            "locked_filters": {"order_status": ["COMPLETED"]},
        },
        "revenue_growth_pct": {
            "name": "Revenue Growth (%)",
            "unit": "PCT",
            "type": "percentage",
            "definition": "Window-like comparison between period aggregates",
            "locked_filters": {"order_status": ["COMPLETED"]},
        },
    }

    def __init__(self, db_path: Path, csv_path: Path) -> None:
        self.db_path = db_path
        self.csv_path = csv_path
        
        # Verify DB exists
        if not db_path.exists():
            raise FileNotFoundError(f"Database warehouse not found: {db_path}. Please run migration script first.")

        with self._get_conn() as conn:
            # Metadata for the dashboard
            self.date_min = _parse_date(conn.execute("SELECT MIN(order_date) FROM orders").fetchone()[0])
            self.date_max = _parse_date(conn.execute("SELECT MAX(order_date) FROM orders").fetchone()[0])
            self.regions = sorted([r[0] for r in conn.execute("SELECT DISTINCT region FROM orders WHERE region IS NOT NULL").fetchall()])
            self.region_groups = sorted([r[0] for r in conn.execute("SELECT DISTINCT region_group FROM orders WHERE region_group IS NOT NULL").fetchall()])
            self.order_statuses = sorted([r[0] for r in conn.execute("SELECT DISTINCT order_status FROM orders WHERE order_status IS NOT NULL").fetchall()])
            
            # Pre-cache customers for search performance
            self.customers = [
                {"customer_id": r[0], "customer_name": r[1]} 
                for r in conn.execute("SELECT DISTINCT customer_id, customer_name FROM orders ORDER BY customer_name").fetchall()
            ]

        # Load CSV for "Legacy" mode
        print(f"Loading Legacy Data Lake from {csv_path}...")
        self.df_cache = pd.read_csv(csv_path)
        self.df_cache['order_date'] = pd.to_datetime(self.df_cache['order_date']).dt.date
        self.df_cache = self.df_cache.rename(columns={'customer': 'customer_id', 'amount_usd': 'revenue_usd'})

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def registry(self) -> dict[str, Any]:
        return {
            "metrics": [
                {"metric_id": k, **v} for k, v in sorted(self.METRICS.items(), key=lambda kv: kv[0])
            ],
            "dimensions": {
                "region": { "values": self.regions },
                "customer": { "key": "customer_id", "display": "customer_name" },
            },
        }

    def meta(self) -> dict[str, Any]:
        with self._get_conn() as conn:
            row_count = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        
        mtime = self.db_path.stat().st_mtime if self.db_path.exists() else None
        return {
            "dataset": {
                "path": str(self.db_path.name),
                "rows": row_count,
                "last_modified": dt.datetime.fromtimestamp(mtime).isoformat() if mtime else None,
            },
            "date_min": _date_to_iso(self.date_min),
            "date_max": _date_to_iso(self.date_max),
            "regions": self.regions,
            "order_statuses": self.order_statuses,
        }

    def customer_search(self, q: str, limit: int = 20) -> list[dict[str, str]]:
        query = q.strip().lower()
        if not query:
            return self.customers[:limit]
        
        # Use SQL LIKE for professional searching
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT DISTINCT customer_id, customer_name FROM orders WHERE customer_name LIKE ? LIMIT ?",
                (f"%{query}%", limit)
            ).fetchall()
            return [{"customer_id": r["customer_id"], "customer_name": r["customer_name"]} for r in rows]

    def _filter_df(self, df: pd.DataFrame, filters: dict[str, Any], status_override: list[str] | None = None) -> pd.DataFrame:
        d = df.copy()
        dr = filters.get("date_range", {})
        if dr.get("start"):
            d = d[d['order_date'] >= _parse_date(dr["start"])]
        if dr.get("end"):
            d = d[d['order_date'] <= _parse_date(dr["end"])]
        if filters.get("region"):
            d = d[d['region'].isin(filters["region"])]
        if filters.get("customer_id"):
            d = d[d['customer_id'].isin(filters["customer_id"])]
        
        st = status_override if status_override is not None else filters.get("order_status")
        if st:
            d = d[d['order_status'].isin(st)]
        return d

    def _query_metric_csv(self, payload: dict[str, Any]) -> dict[str, Any]:
        metric_id = payload["metric_id"]
        filters = payload.get("filters") or {}
        time_grain = payload.get("time_grain") or "month"
        
        date_start = _parse_date(filters.get("date_range", {}).get("start"))
        date_end = _parse_date(filters.get("date_range", {}).get("end"))
        
        # 1. Current value
        df_curr = self._filter_df(self.df_cache, filters, status_override=["COMPLETED"])
        if metric_id == "profit_margin_pct":
            rev = df_curr['revenue_usd'].sum()
            value = (df_curr['profit_usd'].sum() / rev * 100) if rev else 0
        else:
            value = df_curr['revenue_usd'].sum() if metric_id.startswith('revenue') else df_curr['profit_usd'].sum()

        # 2. Previous period
        prev_start, prev_end = self._previous_period(date_start, date_end)
        prev_filters = {**filters, "date_range": {"start": prev_start.isoformat(), "end": prev_end.isoformat()}}
        df_prev = self._filter_df(self.df_cache, prev_filters, status_override=["COMPLETED"])
        if metric_id == "profit_margin_pct":
            rev_p = df_prev['revenue_usd'].sum()
            comparison_value = (df_prev['profit_usd'].sum() / rev_p * 100) if rev_p else 0
        else:
            comparison_value = df_prev['revenue_usd'].sum() if metric_id.startswith('revenue') else df_prev['profit_usd'].sum()

        # 3. Series & Breakdown
        group_by = payload.get("group_by") or "none"
        breakdown = []
        series = []
        
        if group_by == "region":
            bg = df_curr.groupby('region')['revenue_usd' if metric_id.startswith('revenue') else 'profit_usd'].sum().reset_index()
            breakdown = [{"key": r['region'], "value": r[1]} for _, r in bg.iterrows()]
        
        # Simplified series for CSV mode
        series = [{"period_start": date_start.isoformat(), "value": value}]

        delta = value - comparison_value
        return {
            "metric_id": metric_id,
            "value": float(value),
            "comparison_value": float(comparison_value),
            "delta": float(delta),
            "delta_unit": "pp" if metric_id == "profit_margin_pct" else self.METRICS[metric_id]["unit"],
            "series": series,
            "breakdown": breakdown,
            "comparison_period": {"start": prev_start.isoformat(), "end": prev_end.isoformat()}
        }

    def _build_where_clause(self, filters: dict[str, Any], status_override: Optional[list[str]] = None) -> tuple[str, list[Any]]:
        conditions = []
        params = []

        date_range = filters.get("date_range") or {}
        if date_range.get("start"):
            conditions.append("order_date >= ?")
            params.append(date_range["start"])
        if date_range.get("end"):
            conditions.append("order_date <= ?")
            params.append(date_range["end"])

        if filters.get("region"):
            placeholders = ",".join(["?"] * len(filters["region"]))
            conditions.append(f"region IN ({placeholders})")
            params.extend(filters["region"])

        if filters.get("customer_id"):
            placeholders = ",".join(["?"] * len(filters["customer_id"]))
            conditions.append(f"customer_id IN ({placeholders})")
            params.extend(filters["customer_id"])

        statuses = status_override if status_override is not None else filters.get("order_status")
        if statuses:
            placeholders = ",".join(["?"] * len(statuses))
            conditions.append(f"order_status IN ({placeholders})")
            params.extend([s.upper() for s in statuses])

        where = " AND ".join(conditions) if conditions else "1=1"
        return where, params

    def query_metric(self, payload: dict[str, Any], source_mode: str = "sql") -> dict[str, Any]:
        metric_id = payload.get("metric_id")
        if metric_id not in self.METRICS:
            raise ValueError(f"Unsupported metric_id: {metric_id}")

        if source_mode == "csv":
            return self._query_metric_csv(payload)

        filters = payload.get("filters") or {}
        date_start = _parse_date(filters.get("date_range", {}).get("start"))
        date_end = _parse_date(filters.get("date_range", {}).get("end"))
        
        if not date_start or not date_end:
            raise ValueError("Start and end dates are required")

        time_grain = payload.get("time_grain") or "month"
        comparison = payload.get("comparison")

        # 1. Current value
        value = self._get_sql_metric(metric_id, filters)
        
        # 2. Previous period value (for Delta)
        prev_start, prev_end = self._previous_period(date_start, date_end)
        prev_filters = {**filters, "date_range": {"start": prev_start.isoformat(), "end": prev_end.isoformat()}}
        comparison_value = self._get_sql_metric(metric_id, prev_filters)

        # 3. Time series
        series = self._get_sql_series(metric_id, filters, time_grain)

        delta = (value - comparison_value) if (value is not None and comparison_value is not None) else None
        delta_pct = None
        if metric_id != "profit_margin_pct" and comparison_value:
            delta_pct = (delta / comparison_value) * 100

        result = {
            "metric_id": metric_id,
            "value": value,
            "comparison_value": comparison_value,
            "delta": delta,
            "delta_unit": "pp" if metric_id == "profit_margin_pct" else self.METRICS[metric_id]["unit"],
            "delta_pct": delta_pct,
            "series": series,
            "comparison_period": {"start": prev_start.isoformat(), "end": prev_end.isoformat()}
        }

        if metric_id == "profit_margin_pct":
            series_values = [s["value"] for s in series if s["value"] is not None]
            result["distribution"] = _quantile_box(series_values)

        # 4. Dimension breakdown (if requested)
        group_by = payload.get("group_by")
        if group_by and group_by not in ["none", "order_date"]:
            result["breakdown"] = self._get_sql_breakdown(metric_id, filters, group_by)

        return result

    def _get_sql_metric(self, metric_id: str, filters: dict[str, Any]) -> float | None:
        where, params = self._build_where_clause(filters, status_override=["COMPLETED"])
        
        sql_map = {
            "revenue_total_usd": "SELECT SUM(revenue_usd) FROM orders WHERE ",
            "profit_total_usd":  "SELECT SUM(profit_usd) FROM orders WHERE ",
            "profit_margin_pct": "SELECT (SUM(profit_usd) / SUM(revenue_usd)) * 100 FROM orders WHERE ",
            "revenue_growth_pct": "SELECT SUM(revenue_usd) FROM orders WHERE " # handled differently in growth logic
        }
        
        with self._get_conn() as conn:
            row = conn.execute(sql_map[metric_id] + where, params).fetchone()
            return row[0] if row and row[0] is not None else 0.0

    def _get_sql_breakdown(self, metric_id: str, filters: dict[str, Any], group_by: str) -> list[dict[str, Any]]:
        where, params = self._build_where_clause(filters, status_override=["COMPLETED"])
        
        agg_map = {
            "revenue_total_usd": "SUM(revenue_usd)",
            "profit_total_usd":  "SUM(profit_usd)",
            "profit_margin_pct": "(SUM(profit_usd) / SUM(revenue_usd)) * 100",
            "revenue_growth_pct": "SUM(revenue_usd)"
        }

        sql = f"""
            SELECT {group_by} as key, {agg_map[metric_id]} as val
            FROM orders
            WHERE {where}
            GROUP BY key
            ORDER BY val DESC
        """
        
        with self._get_conn() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [{"key": r["key"], "value": r["val"]} for r in rows]

    def _get_sql_series(self, metric_id: str, filters: dict[str, Any], time_grain: str) -> list[dict[str, Any]]:
        where, params = self._build_where_clause(filters, status_override=["COMPLETED"])
        
        # SQLite professional date grouping
        date_fmt = {
            "day": "%Y-%m-%d",
            "week": "%Y-%W", # ISO week approx
            "month": "%Y-%m-01"
        }[time_grain]

        agg_map = {
            "revenue_total_usd": "SUM(revenue_usd)",
            "profit_total_usd":  "SUM(profit_usd)",
            "profit_margin_pct": "(SUM(profit_usd) / SUM(revenue_usd)) * 100",
            "revenue_growth_pct": "SUM(revenue_usd)" # logic applies later
        }

        sql = f"""
            SELECT strftime(?, order_date) as bucket, {agg_map[metric_id]} as val
            FROM orders
            WHERE {where}
            GROUP BY bucket
            ORDER BY bucket ASC
        """
        
        with self._get_conn() as conn:
            rows = conn.execute(sql, [date_fmt] + params).fetchall()
            out = []
            prev_val = None
            for r in rows:
                val = r["val"]
                if metric_id == "revenue_growth_pct":
                    display_val = ((val - prev_val) / prev_val * 100) if prev_val else None
                    prev_val = val
                else:
                    display_val = val
                out.append({"period_start": r["bucket"], "value": display_val})
            return out

    def query_orders(self, payload: dict[str, Any], source_mode: str = "sql") -> dict[str, Any]:
        filters = payload.get("filters") or {}
        
        if source_mode == "csv":
            df = self._filter_df(self.df_cache, filters)
            rows = df.sort_values(['order_date', 'customer_id']).to_dict('records')
            return {"rows": rows, "row_count": len(rows)}

        where, params = self._build_where_clause(filters)
        
        sql = f"SELECT * FROM orders WHERE {where} ORDER BY order_date ASC, order_id ASC"
        
        with self._get_conn() as conn:
            rows = conn.execute(sql, params).fetchall()
            return {
                "rows": [dict(r) for r in rows],
                "row_count": len(rows)
            }

    @staticmethod
    def _previous_period(date_start: dt.date, date_end: dt.date) -> tuple[dt.date, dt.date]:
        days = (date_end - date_start).days + 1
        prev_end = date_start - dt.timedelta(days=1)
        prev_start = prev_end - dt.timedelta(days=days - 1)
        return prev_start, prev_end


class DashboardHandler(SimpleHTTPRequestHandler):
    server_version = "RevenueDashboard/1.0"

    def end_headers(self) -> None:
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def _send_json(self, status: int, payload: Any) -> None:
        body = _json_dumps(payload)
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_error_json(self, status: int, message: str) -> None:
        self._send_json(status, {"error": {"message": message, "status": status}})

    def do_OPTIONS(self) -> None:
        # Same-origin by default, but allow preflight for local dev tools.
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/health":
            return self._send_json(HTTPStatus.OK, {"status": "ok"})
        if parsed.path == "/api/registry":
            return self._send_json(HTTPStatus.OK, SEMANTIC.registry())
        if parsed.path == "/api/meta":
            return self._send_json(HTTPStatus.OK, SEMANTIC.meta())
        if parsed.path == "/api/customers":
            qs = urllib.parse.parse_qs(parsed.query)
            q = (qs.get("q") or [""])[0]
            try:
                limit = int((qs.get("limit") or ["20"])[0])
            except ValueError:
                limit = 20
            return self._send_json(HTTPStatus.OK, {"customers": SEMANTIC.customer_search(q, limit=limit)})
        return super().do_GET()

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        source_mode = self.headers.get("X-Source-Mode", "sql").lower()

        if parsed.path == "/api/query":
            try:
                payload = _read_json_body(self)
                if not isinstance(payload, dict):
                    raise ValueError("Body must be a JSON object")
                out = SEMANTIC.query_metric(payload, source_mode=source_mode)
                return self._send_json(HTTPStatus.OK, out)
            except ValueError as e:
                print(f"400 Bad Request: {e}")
                return self._send_error_json(HTTPStatus.BAD_REQUEST, str(e))
            except Exception as e:  # noqa: BLE001
                return self._send_error_json(HTTPStatus.INTERNAL_SERVER_ERROR, f"Server error: {e}")

        if parsed.path == "/api/orders":
            try:
                payload = _read_json_body(self)
                if not isinstance(payload, dict):
                    raise ValueError("Body must be a JSON object")
                out = SEMANTIC.query_orders(payload, source_mode=source_mode)
                return self._send_json(HTTPStatus.OK, out)
            except ValueError as e:
                return self._send_error_json(HTTPStatus.BAD_REQUEST, str(e))
            except Exception as e:  # noqa: BLE001
                return self._send_error_json(HTTPStatus.INTERNAL_SERVER_ERROR, f"Server error: {e}")

        return self._send_error_json(HTTPStatus.NOT_FOUND, "Unknown endpoint")


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def main() -> int:
    root = _project_root()
    public_dir = root / "public"
    db_path = root / "data" / "warehouse.db"
    csv_path = root / "data" / "validated_data.csv"

    if not public_dir.exists():
        raise FileNotFoundError(f"Missing public directory: {public_dir}")

    global SEMANTIC  # noqa: PLW0603
    SEMANTIC = SemanticLayer(db_path, csv_path)

    host = os.environ.get("DASH_HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", os.environ.get("DASH_PORT", "8787")))

    handler = functools.partial(DashboardHandler, directory=str(public_dir))
    httpd = ThreadingHTTPServer((host, port), handler)

    print(f"Semantic API: http://{host}:{port}/api/registry")
    print(f"Dashboard:   http://{host}:{port}/")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        return 0
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

