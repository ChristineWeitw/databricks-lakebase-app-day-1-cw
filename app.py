"""
Databricks App boilerplate:
- Serves a small Flask API
- Reads/writes to Lakebase (Databricks-managed Postgres) via lakebase.py
- Pulls data from the Massive API via massive_client.py and syncs it into Lakebase

Run locally:
    python app.py
Deploy as a Databricks App using app.yaml.
"""

import logging
import os
import re

import requests
from databricks.sdk import WorkspaceClient
from flask import Flask, jsonify, render_template, request

import lakebase
from massive_client import MassiveClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("massive-app")

app = Flask(__name__)
_w = WorkspaceClient()

TABLE_NAME = os.environ.get("MASSIVE_TABLE_NAME", "massive_records")
WATCHLIST_TABLE_NAME = os.environ.get("WATCHLIST_TABLE_NAME", "watchlist")
TICKETS_TABLE_NAME = os.environ.get("TICKETS_TABLE_NAME", "tickets")
TICKET_MESSAGES_TABLE_NAME = os.environ.get("TICKET_MESSAGES_TABLE_NAME", "ticket_messages")

# Finnhub secret configuration
FINNHUB_SECRET_SCOPE = os.environ.get("FINNHUB_SECRET_SCOPE", "finnhub")
FINNHUB_SECRET_KEY = os.environ.get("FINNHUB_SECRET_KEY", "api-key")

# Basic stock ticker shape check: 1-10 uppercase letters, with an optional
# ".X" or ".XX" share-class suffix (e.g. "BRK.B"). This rejects obviously
# malformed input before we even call the Massive API.
_TICKER_RE = re.compile(r"^[A-Z]{1,10}(\.[A-Z]{1,2})?$")


def ensure_table():
    """Create the destination table in Lakebase if it doesn't exist yet."""
    lakebase.run_write(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id TEXT PRIMARY KEY,
            payload JSONB NOT NULL,
            synced_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )


def ensure_watchlist_table():
    """Create the watchlist table in Lakebase if it doesn't exist yet."""
    lakebase.run_write(
        f"""
        CREATE TABLE IF NOT EXISTS {WATCHLIST_TABLE_NAME} (
            symbol TEXT NOT NULL,
            email TEXT NOT NULL,
            latest_price NUMERIC,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (symbol, email)
        )
        """
    )


def ensure_tickets_table():
    """Create the tickets table in Lakebase if it doesn't exist yet."""
    lakebase.run_write(
        f"""
        CREATE TABLE IF NOT EXISTS {TICKETS_TABLE_NAME} (
            ticket_id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            created_by TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )


def ensure_ticket_messages_table():
    """Create the ticket_messages table in Lakebase if it doesn't exist yet."""
    lakebase.run_write(
        f"""
        CREATE TABLE IF NOT EXISTS {TICKET_MESSAGES_TABLE_NAME} (
            message_id SERIAL PRIMARY KEY,
            ticket_id TEXT NOT NULL,
            message_text TEXT NOT NULL,
            author TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            FOREIGN KEY (ticket_id) REFERENCES {TICKETS_TABLE_NAME}(ticket_id)
        )
        """
    )


def _current_user_email() -> str:
    """
    Resolve the current user's email so the watchlist can be personalized.

    Databricks Apps inject the logged-in user's identity via the
    X-Forwarded-Email header on every request. Fall back to the Databricks
    SDK's current_user API for local development where that header isn't set.
    """
    header_email = request.headers.get("X-Forwarded-Email")
    if header_email:
        return header_email
    return _w.current_user.me().user_name


def _get_finnhub_api_key() -> str:
    """
    Fetch the Finnhub API key from Databricks secrets.
    Falls back to 'demo' for local development (though demo keys don't work).
    """
    try:
        import base64
        secret = _w.secrets.get_secret(scope=FINNHUB_SECRET_SCOPE, key=FINNHUB_SECRET_KEY)
        return base64.b64decode(secret.value).decode("utf-8")
    except Exception as e:
        logger.warning(f"Could not fetch Finnhub API key from secrets: {e}")
        return "demo"  # Fallback for local dev (won't work but prevents crashes)


def fetch_stock_news(symbol: str, limit: int = 10) -> list[dict]:
    """
    Fetch stock news from Finnhub API for the given symbol.
    
    Returns a list of news articles with headline, summary, source, and datetime.
    Get a free API key at https://finnhub.io (60 calls/minute on free tier).
    """
    url = f"https://finnhub.io/api/v1/company-news"
    
    # Get news from the last 30 days
    from datetime import datetime, timedelta
    to_date = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    
    params = {
        "symbol": symbol,
        "from": from_date,
        "to": to_date,
        "token": _get_finnhub_api_key()
    }
    
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        news_data = resp.json()
        
        # Limit results and format for our use case
        return [
            {
                "headline": item.get("headline", ""),
                "summary": item.get("summary", ""),
                "source": item.get("source", ""),
                "url": item.get("url", ""),
                "datetime": item.get("datetime", 0)  # Unix timestamp
            }
            for item in (news_data if isinstance(news_data, list) else [])[:limit]
        ]
    except requests.RequestException as e:
        logger.error(f"Failed to fetch news for {symbol}: {e}")
        return []


@app.route("/healthz")
def healthz():
    return jsonify({"status": "ok"})


@app.errorhandler(Exception)
def handle_exception(err):
    """Ensure all unhandled errors return JSON (not an HTML error page),
    so the frontend's resp.json() call never chokes on HTML."""
    logger.exception("Unhandled exception while processing request")
    status_code = getattr(err, "code", 500)
    if not isinstance(status_code, int):
        status_code = 500
    return jsonify({"error": str(err)}), status_code


@app.route("/")
def index():
    """Simple UI to submit a list of stock symbols to sync from Massive."""
    return render_template("index.html")


@app.route("/records")
def list_records():
    """Read records already synced into Lakebase."""
    limit = int(request.args.get("limit", 100))
    rows = lakebase.run_query(
        f"SELECT id, payload, synced_at FROM {TABLE_NAME} ORDER BY synced_at DESC LIMIT %s",
        (limit,),
    )
    return jsonify(rows)


@app.route("/sync", methods=["POST"])
def sync_from_massive():
    """
    Pull data from the Massive API (paginated, potentially huge dataset) and
    upsert it into Lakebase in batches.
    """
    ensure_table()
    client = MassiveClient()

    path = request.json.get("path", "/records") if request.is_json else "/records"
    batch_size = int(request.args.get("batch_size", 500))

    batch = []
    total = 0
    for item in client.paginated_get(path):
        batch.append(item)
        if len(batch) >= batch_size:
            total += _upsert_batch(batch)
            batch = []

    if batch:
        total += _upsert_batch(batch)

    return jsonify({"synced": total})


@app.route("/watchlist", methods=["GET"])
def get_watchlist():
    """Return the current user's watchlist symbols, with their last known price."""
    ensure_watchlist_table()
    email = _current_user_email()
    rows = lakebase.run_query(
        f"SELECT symbol, email, latest_price, updated_at FROM {WATCHLIST_TABLE_NAME} "
        f"WHERE email = %s ORDER BY symbol ASC",
        (email,),
    )
    return jsonify(rows)


@app.route("/watchlist/<symbol>", methods=["DELETE"])
def delete_from_watchlist(symbol):
    """
    Remove a symbol from the current user's watchlist.
    """
    ensure_watchlist_table()
    email = _current_user_email()
    symbol = symbol.strip().upper() if isinstance(symbol, str) else ""

    if not symbol or not _TICKER_RE.match(symbol):
        return jsonify({"error": f"Invalid ticker symbol: {symbol!r}"}), 400

    lakebase.run_write(
        f"DELETE FROM {WATCHLIST_TABLE_NAME} WHERE symbol = %s AND email = %s",
        (symbol, email),
    )

    return jsonify({"symbol": symbol, "status": "deleted"})


@app.route("/ticker/<symbol>/news", methods=["POST"])
def get_stock_news(symbol):
    """
    Fetch stock news for a ticker symbol from Finnhub API and store it in
    the ticket_messages table. Also ensures the ticker exists in tickets table.
    """
    ensure_tickets_table()
    ensure_ticket_messages_table()
    
    symbol = symbol.strip().upper() if isinstance(symbol, str) else ""
    
    if not symbol or not _TICKER_RE.match(symbol):
        return jsonify({"error": f"Invalid ticker symbol: {symbol!r}"}), 400
    
    email = _current_user_email()
    
    # Ensure the ticket exists
    lakebase.run_write(
        f"""
        INSERT INTO {TICKETS_TABLE_NAME} (ticket_id, title, status, created_by, created_at)
        VALUES (%s, %s, %s, %s, now())
        ON CONFLICT (ticket_id) DO NOTHING
        """,
        (symbol, f"{symbol} Stock", "active", email),
    )
    
    # Fetch news from Finnhub
    news_articles = fetch_stock_news(symbol, limit=10)
    
    if not news_articles:
        return jsonify({"error": f"No news found for ticker: {symbol}"}), 404
    
    # Store news articles in ticket_messages table
    import json as _json
    from datetime import datetime
    
    stored_count = 0
    for article in news_articles:
        message_text = _json.dumps({
            "headline": article.get("headline", ""),
            "summary": article.get("summary", ""),
            "url": article.get("url", ""),
            "datetime": article.get("datetime", 0)
        })
        author = article.get("source", "Unknown")
        
        # Convert Unix timestamp to PostgreSQL timestamp
        article_time = datetime.fromtimestamp(article.get("datetime", 0)) if article.get("datetime") else datetime.now()
        
        lakebase.run_write(
            f"""
            INSERT INTO {TICKET_MESSAGES_TABLE_NAME} (ticket_id, message_text, author, created_at)
            VALUES (%s, %s, %s, %s)
            """,
            (symbol, message_text, author, article_time),
        )
        stored_count += 1
    
    return jsonify({
        "symbol": symbol,
        "news_count": stored_count,
        "news": news_articles
    })


@app.route("/watchlist", methods=["POST"])
def add_to_watchlist():
    """
    Fetch the latest price for a single stock symbol from Massive using
    exactly ONE API call (see MassiveClient.get_latest_price), then add/
    update that symbol on the watchlist in Lakebase.
    Also creates/updates a ticket entry for this symbol.
    """
    ensure_watchlist_table()
    ensure_tickets_table()

    if request.is_json:
        symbol = request.json.get("symbol", "")
    else:
        symbol = request.form.get("symbol", "")

    symbol = symbol.strip().upper() if isinstance(symbol, str) else ""

    if not symbol or not _TICKER_RE.match(symbol):
        return jsonify({"error": f"Invalid ticker symbol: {symbol!r}"}), 400

    client = MassiveClient()
    try:
        data = client.get_latest_price(symbol)  # <-- single API call, latest price only
    except requests.HTTPError:
        # Massive returns a 404/4xx for tickers it doesn't recognize.
        return jsonify({"error": f"Unknown ticker symbol: {symbol}"}), 400

    price = _extract_latest_price(data)
    if price is None:
        # No usable price in the response (e.g. delisted/invalid ticker
        # that still 200s with an empty result set) - don't add it.
        return jsonify({"error": f"No price data available for ticker: {symbol}"}), 400

    email = _current_user_email()

    # Insert/update the ticket entry
    lakebase.run_write(
        f"""
        INSERT INTO {TICKETS_TABLE_NAME} (ticket_id, title, status, created_by, created_at)
        VALUES (%s, %s, %s, %s, now())
        ON CONFLICT (ticket_id) DO UPDATE
            SET status = EXCLUDED.status
        """,
        (symbol, f"{symbol} Stock", "active", email),
    )

    # Insert/update the watchlist entry
    lakebase.run_write(
        f"""
        INSERT INTO {WATCHLIST_TABLE_NAME} (symbol, email, latest_price, updated_at)
        VALUES (%s, %s, %s, now())
        ON CONFLICT (symbol, email) DO UPDATE
            SET latest_price = EXCLUDED.latest_price,
                updated_at = EXCLUDED.updated_at
        """,
        (symbol, email, price),
    )

    return jsonify({"symbol": symbol, "email": email, "latest_price": price})


def _extract_latest_price(data: dict) -> float | None:
    """Pull the trade price out of the Massive 'previous close' response shape.

    The /v2/aggs/ticker/{symbol}/prev endpoint returns "results" as a LIST
    containing a single aggregate bar (not a dict), e.g.:
        {"status": "OK", "resultsCount": 1, "results": [{"c": 148.845, ...}]}
    Previously this code treated "results" as a dict, so isinstance(results, dict)
    was always False for this endpoint's real shape and the price silently
    resolved to None. Unwrap the list here, and check "status"/"resultsCount"
    so invalid tickers (empty results) are detected instead of "succeeding"
    with a null price.

    Adjust the key lookup here if the real Massive API returns a different
    field name for the traded/close price.
    """
    if not isinstance(data, dict):
        return None
    if data.get("status") not in (None, "OK") or data.get("resultsCount") == 0:
        return None
    results = data.get("results", data)
    if isinstance(results, list):
        results = results[0] if results else None
    if isinstance(results, dict):
        for key in ("c", "p", "price", "last_price", "vw"):
            if key in results:
                return results[key]
    return None


def _upsert_batch(items: list[dict]) -> int:
    """Upsert a batch of Massive API items into Lakebase, one statement per row.

    For very large batches, consider psycopg2.extras.execute_values for
    higher throughput instead of per-row execute calls.
    """
    import json as _json

    count = 0
    with lakebase.get_connection() as conn:
        with conn.cursor() as cur:
            for item in items:
                cur.execute(
                    f"""
                    INSERT INTO {TABLE_NAME} (id, payload, synced_at)
                    VALUES (%s, %s, now())
                    ON CONFLICT (id) DO UPDATE
                        SET payload = EXCLUDED.payload,
                            synced_at = EXCLUDED.synced_at
                    """,
                    (str(item.get("id")), _json.dumps(item)),
                )
                count += 1
            conn.commit()
    return count


if __name__ == '__main__':
    host = os.getenv('FLASK_RUN_HOST', '0.0.0.0')
    port = int(os.getenv('FLASK_RUN_PORT', 8000))
    app.run(debug=True, host=host, port=port)
    print(f"Flask app running on http://{host}:{port}")