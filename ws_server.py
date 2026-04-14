"""
WebSocket Live Streaming Server for Nifty 50 Chart
===================================================
Serves the chart page via HTTP and pushes live LTP ticks via WebSocket.
Runs a live strategy and persists trades to Upstash Redis.

Usage:
    python ws_server.py                          # default port 8765, strategy v7
    python ws_server.py --port 9000              # custom port
    python ws_server.py --strategy v6            # choose strategy

Architecture:
    - HTTP GET /          → serves the live chart HTML (with embedded WS client)
    - HTTP GET /backtest  → backtest page
    - HTTP GET /trades    → live trades dashboard
    - WebSocket /ws       → pushes {"type":"tick", ...} every ~2 seconds
"""

import os
import sys
import json
import asyncio
import logging
import argparse
from datetime import datetime

import pytz
from dotenv import load_dotenv

# ── Path setup ──────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lib.engine import (
    login, fetch_candles, generate_chart_data, render_live_chart_html,
    get_ltp, IST, is_market_open,
    fetch_candles_multiday, generate_backtest_data, render_backtest_html,
    run_backtest, _compute_ribbon_state,
)
from lib.trade_db import save_trade, save_open_position, get_dashboard_data

# ── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Credentials ─────────────────────────────────────────────────────────────
load_dotenv()
API_KEY     = os.getenv("API_KEY", "").strip()
CLIENT_ID   = os.getenv("CLIENT_ID", "").strip()
PASSWORD    = os.getenv("PASSWORD", "").strip()
TOTP_SECRET = (os.getenv("TOTP_SECRET", "") or os.getenv("TOTP", "")).strip()

# ── State ───────────────────────────────────────────────────────────────────
POLL_INTERVAL = 2          # seconds between LTP polls
connected_clients = set()  # active WebSocket connections
chart_html_cache = None    # cached HTML for HTTP serving

# Current-minute candle being built from ticks
current_candle = None      # {"time": unix, "open": ..., "high": ..., "low": ..., "close": ...}
current_minute = None      # datetime minute (floored)

# ── Live strategy state ───────────────────────────────────────────────────
live_strategy = "v7"       # set via --strategy flag
live_display_df = None     # updated on each new candle
live_full_df = None
prev_trade_counts = {"v1": 0, "v7": 0, "v8": 0}       # track how many closed trades we've already saved


async def broadcast(message: dict):
    """Send a JSON message to all connected WebSocket clients."""
    global connected_clients
    if not connected_clients:
        return
    data = json.dumps(message)
    dead = set()
    for ws in connected_clients:
        try:
            await ws.send(data)
        except Exception:
            dead.add(ws)
    connected_clients -= dead


async def ltp_poller(smart_obj):
    """Background task: poll LTP every POLL_INTERVAL and broadcast ticks."""
    global current_candle, current_minute

    log.info("LTP poller started (every %ds)", POLL_INTERVAL)

    while True:
        try:
            if not is_market_open():
                log.info("Market closed — poller sleeping 60s")
                await asyncio.sleep(60)
                continue

            ltp = await asyncio.get_event_loop().run_in_executor(
                None, get_ltp, smart_obj
            )

            if ltp is None:
                log.warning("LTP fetch returned None, skipping tick")
                await asyncio.sleep(POLL_INTERVAL)
                continue

            now_ist = datetime.now(IST)
            minute_ts = now_ist.replace(second=0, microsecond=0)
            # Unix timestamp for Lightweight Charts
            unix_time = int(minute_ts.timestamp())

            if current_minute is None or minute_ts > current_minute:
                # ── New minute ──────────────────────────────────────────
                if current_candle is not None:
                    log.info(
                        "Candle closed: %s O=%.2f H=%.2f L=%.2f C=%.2f",
                        current_minute.strftime("%H:%M"),
                        current_candle["open"], current_candle["high"],
                        current_candle["low"], current_candle["close"],
                    )

                    # ── Run live strategy on candle close ───────────────
                    await _run_live_strategy_check()

                # Start a new candle
                current_minute = minute_ts
                current_candle = {
                    "time": unix_time,
                    "open": round(ltp, 2),
                    "high": round(ltp, 2),
                    "low": round(ltp, 2),
                    "close": round(ltp, 2),
                }
                
                # ── Ensure tick updates correctly execute strategy evaluation inter-bar
                await _run_live_strategy_check(is_tick=True)
                
                await broadcast({"type": "new_candle", "candle": current_candle})
                log.info("New candle started: %s LTP=%.2f", minute_ts.strftime("%H:%M"), ltp)

            else:
                # ── Same minute — update the candle ─────────────────────
                current_candle["high"] = round(max(current_candle["high"], ltp), 2)
                current_candle["low"] = round(min(current_candle["low"], ltp), 2)
                current_candle["close"] = round(ltp, 2)
                
                # ── TICK RUN: Ensure inter-bar updates evaluate live widths dynamically
                await _run_live_strategy_check(is_tick=True)

                await broadcast({
                    "type": "tick",
                    "time": current_candle["time"],
                    "open": current_candle["open"],
                    "high": current_candle["high"],
                    "low": current_candle["low"],
                    "close": current_candle["close"],
                    "ltp": round(ltp, 2),
                })

        except Exception as e:
            log.error("Poller error: %s", e)

        await asyncio.sleep(POLL_INTERVAL)


async def ws_handler(websocket):
    """Handle a new WebSocket connection."""
    connected_clients.add(websocket)
    remote = websocket.remote_address
    log.info("WS client connected: %s (%d total)", remote, len(connected_clients))
    try:
        async for _ in websocket:
            pass  # We don't expect messages from the client
    except Exception:
        pass
    finally:
        connected_clients.discard(websocket)
        log.info("WS client disconnected: %s (%d remaining)", remote, len(connected_clients))


# Cached backtest HTML shell (generated once at startup)
backtest_html_cache = None
# Hold reference to smart_obj for backtest API calls
_smart_obj_ref = None


async def _run_live_strategy_check(is_tick: bool = False):
    """Re-run the live strategy on current data; optionally evaluate continuously without closing out finalized trades mid-bar."""
    global prev_trade_counts, live_display_df, live_full_df

    if live_display_df is None or live_full_df is None:
        return

    try:
        # Re-fetch latest candles (lightweight: only adds the new minute)
        import pandas as pd
        if current_candle is not None and current_minute is not None:
            # Drop the active candle if it exists (so we organically overwrite intra-minute spikes)
            ts = pd.Timestamp(current_minute.strftime("%Y-%m-%d %H:%M:%S"))
            
            # Temporary copy of dataframe to append live tick without polluting permanently until candle-close
            temp_display_df = live_display_df.copy()
            temp_full_df = live_full_df.copy()
            
            new_row = pd.DataFrame(
                {"Open": current_candle["open"],
                 "High": current_candle["high"],
                 "Low": current_candle["low"],
                 "Close": current_candle["close"],
                 "Volume": 0},
                index=pd.DatetimeIndex([ts], name="Date"),
            )
            
            # Upsert the tick natively into the active pandas memory footprint
            if ts in temp_display_df.index:
                temp_display_df.update(new_row)
                temp_full_df.update(new_row)
            else:
                temp_display_df = pd.concat([temp_display_df, new_row]).sort_index()
                temp_full_df = pd.concat([temp_full_df, new_row]).sort_index()
                
                # If we officially close out on a non-tick sweep, we append it fully!
                if not is_tick:
                    live_display_df = temp_display_df
                    live_full_df = temp_full_df
            
            temp_display_df = temp_display_df[~temp_display_df.index.duplicated(keep='last')]
            temp_full_df = temp_full_df[~temp_full_df.index.duplicated(keep='last')]

            STRATEGIES_TO_TRACK = ["v1", "v7", "v8"]
            for st in STRATEGIES_TO_TRACK:
                # Run the natively selected multi strategy logic
                trades, stats = await asyncio.get_event_loop().run_in_executor(
                    None, run_backtest, temp_display_df, temp_full_df, st
                )

                closed_trades = [t for t in trades if not t.get("open")]
                open_trades = [t for t in trades if t.get("open")]

                if st not in prev_trade_counts:
                    prev_trade_counts[st] = 0

                # Strictly only execute finalized Save-to-DB calls on absolute completed candle borders preventing duplicate paints
                if not is_tick:
                    current_prev = prev_trade_counts[st]
                    if len(closed_trades) > current_prev:
                        for t in closed_trades[current_prev:]:
                            t["strategy"] = st
                            save_trade(t, strategy=st)
                            log.info("💾 [%s] Trade saved to DB: %s %s pts", st, t["type"], t["points"])
                        prev_trade_counts[st] = len(closed_trades)

                # Open positions intuitively update inter-bar instantly for highest accuracy display tracking
                if open_trades:
                    pos = open_trades[-1]
                    pos["strategy"] = st
                    save_open_position(pos, strategy=st)
                else:
                    save_open_position(None, strategy=st)

    except Exception as e:
        log.error("Live strategy check failed: %s", e, exc_info=True)


async def process_request(connection, request):
    """
    Intercept HTTP requests before WebSocket upgrade.
    Serves the chart HTML for root path; lets /ws through for WS upgrade.
    Uses websockets v16+ API: connection.respond().
    """
    import http
    from urllib.parse import urlparse, parse_qs

    parsed = urlparse(request.path)
    path = parsed.path.rstrip("/") or "/"

    if path == "/":
        if chart_html_cache:
            response = connection.respond(http.HTTPStatus.OK, chart_html_cache)
            response.headers["Content-Type"] = "text/html; charset=utf-8"
            response.headers["Cache-Control"] = "no-cache"
            return response
        else:
            response = connection.respond(http.HTTPStatus.SERVICE_UNAVAILABLE, "Chart not ready yet.")
            response.headers["Content-Type"] = "text/plain"
            return response

    elif path == "/backtest":
        try:
            html = render_backtest_html(chart_href="/", api_base="/api/backtest")
            response = connection.respond(http.HTTPStatus.OK, html)
            response.headers["Content-Type"] = "text/html; charset=utf-8"
            response.headers["Cache-Control"] = "no-cache"
            return response
        except Exception as e:
            response = connection.respond(http.HTTPStatus.SERVICE_UNAVAILABLE, "Backtest page not ready yet.")
            response.headers["Content-Type"] = "text/plain"
            return response

    elif path == "/api/backtest":
        # Run backtest on-demand and return JSON
        qs = parse_qs(parsed.query)
        days = int(qs.get("days", ["3"])[0])
        strategy = qs.get("strategy", ["v1"])[0]
        days = max(1, min(days, 30))  # clamp 1..30
        if strategy not in ("v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8"):
            strategy = "v1"

        try:
            log.info("Backtest API: days=%d strategy=%s", days, strategy)
            display_df, full_df = await asyncio.get_event_loop().run_in_executor(
                None, fetch_candles_multiday, _smart_obj_ref, days
            )
            data = await asyncio.get_event_loop().run_in_executor(
                None, generate_backtest_data, display_df, full_df, strategy
            )
            # Convert ribbon_series keys from int to str for JSON
            data["ribbon_series"] = {
                str(k): v for k, v in data["ribbon_series"].items()
            }
            body = json.dumps(data)
            response = connection.respond(http.HTTPStatus.OK, body)
            response.headers["Content-Type"] = "application/json; charset=utf-8"
            response.headers["Cache-Control"] = "no-cache"
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response
        except Exception as e:
            log.error("Backtest API error: %s", e, exc_info=True)
            error_body = json.dumps({"error": str(e)})
            response = connection.respond(http.HTTPStatus.INTERNAL_SERVER_ERROR, error_body)
            response.headers["Content-Type"] = "application/json"
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response

    # Let WebSocket upgrade requests through (path=/ws or any other)

    # ── /trades — live trades dashboard (local server) ─────────────────
    if path == "/trades":
        try:
            data = get_dashboard_data()
            from api.trades import _render_dashboard_html
            html = _render_dashboard_html(data)
            response = connection.respond(http.HTTPStatus.OK, html)
            response.headers["Content-Type"] = "text/html; charset=utf-8"
            response.headers["Cache-Control"] = "no-cache"
            return response
        except Exception as e:
            log.error("Trades dashboard error: %s", e)
            response = connection.respond(http.HTTPStatus.INTERNAL_SERVER_ERROR,
                                          f"Error: {e}")
            response.headers["Content-Type"] = "text/plain"
            return response

    elif path == "/api/trades":
        try:
            data = get_dashboard_data()
            body = json.dumps(data)
            response = connection.respond(http.HTTPStatus.OK, body)
            response.headers["Content-Type"] = "application/json"
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response
        except Exception as e:
            log.error("Trades API error: %s", e)
            error_body = json.dumps({"error": str(e)})
            response = connection.respond(http.HTTPStatus.INTERNAL_SERVER_ERROR,
                                          error_body)
            response.headers["Content-Type"] = "application/json"
            return response

    return None


async def main(port: int):
    global chart_html_cache, backtest_html_cache, _smart_obj_ref
    global live_display_df, live_full_df, live_strategy, prev_trade_count

    import websockets
    from websockets.asyncio.server import serve

    log.info("=" * 60)
    log.info("  Nifty 50 Live WebSocket Server")
    log.info("=" * 60)

    # ── Login ───────────────────────────────────────────────────────────
    log.info("Logging in to AngelOne...")
    smart_obj = await asyncio.get_event_loop().run_in_executor(
        None, login, API_KEY, CLIENT_ID, PASSWORD, TOTP_SECRET
    )
    log.info("Login successful!")
    _smart_obj_ref = smart_obj  # store for backtest API

    # ── Fetch initial candle data ───────────────────────────────────────
    log.info("Fetching initial candle data...")
    display_df, full_df = await asyncio.get_event_loop().run_in_executor(
        None, fetch_candles, smart_obj
    )
    data = generate_chart_data(display_df, full_df)
    ws_url = f"ws://localhost:{port}/ws"
    chart_html_cache = render_live_chart_html(data, ws_url=ws_url,
                                              results_href="results.html")
    log.info("Chart ready: %d candles", data["candles"])

    
    # Store DataFrames for live strategy engine
    live_display_df = display_df.copy()
    live_full_df = full_df.copy()
    prev_trade_counts = {"v1": 0, "v7": 0, "v8": 0}  # reset on server start

    log.info("Server pages initialized")

    # ── Start WebSocket server ──────────────────────────────────────────
    async with serve(
        ws_handler,
        "0.0.0.0",
        port,
        process_request=process_request,
    ):
        log.info("Server running at http://localhost:%d", port)
        log.info("WebSocket endpoint: ws://localhost:%d/ws", port)
        log.info("Backtest page: http://localhost:%d/backtest", port)
        log.info("Trades dashboard: http://localhost:%d/trades", port)
        log.info("Live strategy: %s", live_strategy)
        log.info("Open http://localhost:%d in your browser!", port)
        log.info("-" * 60)

        # Start the LTP polling background task
        poller_task = asyncio.create_task(ltp_poller(smart_obj))

        # Run forever
        try:
            await asyncio.Future()  # block forever
        except asyncio.CancelledError:
            poller_task.cancel()
            log.info("Server shutting down.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Nifty 50 Live WebSocket Server")
    parser.add_argument("--port", type=int, default=8765, help="Port to serve on (default: 8765)")
    parser.add_argument("--strategy", type=str, default="v7",
                        choices=["v1", "v2", "v3", "v4", "v5", "v6", "v7"],
                        help="Strategy version for live trading (default: v7)")
    args = parser.parse_args()
    live_strategy = args.strategy

    try:
        asyncio.run(main(args.port))
    except KeyboardInterrupt:
        log.info("Stopped by user.")
