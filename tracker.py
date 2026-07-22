#!/usr/bin/env python3
"""
Flight price tracker: pulls Google Flights data from SerpApi, compares it
against locally persisted history, and alerts on Telegram when a route gets
cheap. Designed to run headless on a GitHub Actions cron schedule.
"""

import json
import logging
import os
import statistics
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

SERPAPI_ENDPOINT = "https://serpapi.com/search.json"
TELEGRAM_API_BASE = "https://api.telegram.org"

ROUTES_FILE = Path(os.environ.get("ROUTES_FILE", "routes.json"))
HISTORY_FILE = Path(os.environ.get("HISTORY_FILE", "price_history.json"))

TOP_N_FLIGHTS = 5
MAX_HISTORY_ENTRIES_PER_WINDOW = 90
MIN_HISTORY_FOR_MEDIAN_CHECK = 3
DEFAULT_COMBOS_PER_RUN = int(os.environ.get("COMBOS_PER_RUN", "1"))

DROP_THRESHOLD_PCT = float(os.environ.get("DROP_THRESHOLD_PCT", "15"))
HARD_THRESHOLD_MYR = float(os.environ.get("HARD_THRESHOLD_MYR", "1500"))

REQUEST_TIMEOUT_SECONDS = 30
REQUEST_RETRIES = 2
REQUEST_RETRY_BACKOFF_SECONDS = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("flight-tracker")


# --------------------------------------------------------------------------
# SerpApi
# --------------------------------------------------------------------------

def fetch_flight_data(route: dict, api_key: str) -> dict | None:
    """Hit SerpApi's google_flights engine for a single route. Returns the
    parsed JSON payload, or None if the request ultimately failed."""

    params = {
        "engine": "google_flights",
        "departure_id": route["departure_id"],
        "arrival_id": route["arrival_id"],
        "outbound_date": route["outbound_date"],
        "currency": route.get("currency", "MYR"),
        "hl": "en",
        "api_key": api_key,
    }
    if route.get("return_date"):
        params["return_date"] = route["return_date"]
        params["type"] = "1"  # round trip
    else:
        params["type"] = "2"  # one way

    last_error = None
    for attempt in range(1, REQUEST_RETRIES + 2):
        try:
            response = requests.get(
                SERPAPI_ENDPOINT, params=params, timeout=REQUEST_TIMEOUT_SECONDS
            )
            response.raise_for_status()
            data = response.json()

            if "error" in data:
                log.error("SerpApi returned an error: %s", data["error"])
                return None

            return data

        except requests.exceptions.RequestException as exc:
            last_error = exc
            log.warning(
                "SerpApi request failed (attempt %s/%s): %s",
                attempt,
                REQUEST_RETRIES + 1,
                exc,
            )
            if attempt <= REQUEST_RETRIES:
                time.sleep(REQUEST_RETRY_BACKOFF_SECONDS)

    log.error("SerpApi request exhausted retries: %s", last_error)
    return None


def extract_top_flights(data: dict, top_n: int = TOP_N_FLIGHTS) -> list[dict]:
    """Combine best_flights + other_flights, sort by price, and return the
    top_n cheapest options as simplified dicts."""

    candidates = (data.get("best_flights") or []) + (data.get("other_flights") or [])
    priced = [f for f in candidates if isinstance(f.get("price"), (int, float))]
    priced.sort(key=lambda f: f["price"])

    google_flights_url = (data.get("search_metadata") or {}).get("google_flights_url")

    simplified = []
    for flight in priced[:top_n]:
        legs = flight.get("flights") or []
        airline = legs[0]["airline"] if legs and legs[0].get("airline") else "Unknown airline"
        simplified.append(
            {
                "price": flight["price"],
                "airline": airline,
                "total_duration": flight.get("total_duration"),
                "google_flights_url": google_flights_url,
            }
        )
    return simplified


# --------------------------------------------------------------------------
# Price history persistence
# --------------------------------------------------------------------------

def load_price_history() -> dict:
    if not HISTORY_FILE.exists():
        log.info("No existing %s found, starting fresh.", HISTORY_FILE)
        return {}

    try:
        with HISTORY_FILE.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        log.error("Failed to read %s (%s); starting from empty history.", HISTORY_FILE, exc)
        return {}


def save_price_history(history: dict) -> None:
    try:
        with HISTORY_FILE.open("w", encoding="utf-8") as f:
            json.dump(history, f, indent=2, sort_keys=True)
    except OSError as exc:
        log.error("Failed to write %s: %s", HISTORY_FILE, exc)


def window_key(route: dict) -> str:
    """Identifies a route's search window (not a single date pair), so price
    history and the rotation cursor pool across every date/trip-length combo
    the window covers. Changing the window bounds starts a fresh baseline."""
    return "-".join(
        [
            route["departure_id"],
            route["arrival_id"],
            route.get("currency", "MYR"),
            route["earliest_departure"],
            route["latest_departure"],
            str(route["min_nights"]),
            str(route["max_nights"]),
        ]
    )


def route_label(route: dict) -> str:
    return f"{route['departure_id']} -> {route['arrival_id']}"


def generate_date_candidates(route: dict) -> list[dict]:
    """Expand a route's departure window + nights range into every
    (outbound_date, return_date, nights) combo, sorted by departure date then
    trip length."""
    try:
        earliest = datetime.strptime(route["earliest_departure"], "%Y-%m-%d").date()
        latest = datetime.strptime(route["latest_departure"], "%Y-%m-%d").date()
        min_nights = int(route["min_nights"])
        max_nights = int(route["max_nights"])
    except (KeyError, ValueError) as exc:
        log.error("Invalid date window for route %s: %s", route_label(route), exc)
        return []

    if earliest > latest or min_nights > max_nights or min_nights < 1:
        log.error("Invalid date window bounds for route %s.", route_label(route))
        return []

    candidates = []
    day_count = (latest - earliest).days + 1
    for offset in range(day_count):
        departure = earliest + timedelta(days=offset)
        for nights in range(min_nights, max_nights + 1):
            candidates.append(
                {
                    "outbound_date": departure.isoformat(),
                    "return_date": (departure + timedelta(days=nights)).isoformat(),
                    "nights": nights,
                }
            )
    return candidates


# --------------------------------------------------------------------------
# Evaluation logic
# --------------------------------------------------------------------------

def evaluate_deal(route: dict, current_price: float, history_entry: dict) -> dict:
    """Decide whether the current price qualifies as a deal. Returns a dict
    with the decision plus the numbers needed to format the alert."""

    prior_prices = history_entry.get("prices", []) if history_entry else []
    currency = route.get("currency", "MYR")

    median_price = None
    pct_drop = None
    median_deal = False
    if len(prior_prices) >= MIN_HISTORY_FOR_MEDIAN_CHECK:
        median_price = statistics.median(prior_prices)
        if median_price > 0:
            pct_drop = (median_price - current_price) / median_price * 100
            median_deal = pct_drop >= DROP_THRESHOLD_PCT

    hard_deal = currency == "MYR" and current_price <= HARD_THRESHOLD_MYR

    return {
        "is_deal": median_deal or hard_deal,
        "median_deal": median_deal,
        "hard_deal": hard_deal,
        "median_price": median_price,
        "pct_drop": pct_drop,
    }


def update_history_entry(history: dict, route: dict, candidate: dict, current_price: float) -> dict:
    """Append this observation to the window's pooled price history and
    update the best combo seen so far. Returns the updated entry."""
    key = window_key(route)
    entry = history.setdefault(
        key,
        {
            "route_label": route_label(route),
            "currency": route.get("currency", "MYR"),
            "prices": [],
            "best": None,
            "cursor": 0,
        },
    )
    entry["prices"].append(current_price)
    entry["prices"] = entry["prices"][-MAX_HISTORY_ENTRIES_PER_WINDOW:]
    entry["last_updated"] = datetime.now(timezone.utc).isoformat()

    if not entry.get("best") or current_price < entry["best"]["price"]:
        entry["best"] = {
            "price": current_price,
            "outbound_date": candidate["outbound_date"],
            "return_date": candidate["return_date"],
            "nights": candidate["nights"],
        }
    return entry


# --------------------------------------------------------------------------
# Telegram alerting
# --------------------------------------------------------------------------

def escape_markdown(text: str) -> str:
    """Escape special characters for Telegram's legacy Markdown parse mode."""
    for char in ("_", "*", "[", "`"):
        text = text.replace(char, f"\\{char}")
    return text


def format_window_best_line(window_best: dict | None, currency: str) -> str | None:
    if not window_best:
        return None
    return (
        f"🏆 Best in window so far: {window_best['price']:,.0f} {currency} "
        f"on {window_best['outbound_date']} - {window_best['return_date']} "
        f"({window_best['nights']}n)"
    )


def format_batch_message(route: dict, results: list[dict], window_best: dict | None) -> str:
    """Build one consolidated Telegram message covering every date/night
    combo checked this run for a route. `results` is a list of dicts with
    keys: candidate, current_price, top_flight, evaluation (in the order
    they were checked)."""
    label = escape_markdown(route_label(route))
    currency = route.get("currency", "MYR")
    any_deal = any(r["evaluation"]["is_deal"] for r in results)

    header = "🚨 *DEAL ALERT!* 🚨" if any_deal else "✈️ *Price sweep*"
    lines = [header, "", f"*Route:* {label}", f"*Combos checked:* {len(results)}", ""]

    sorted_results = sorted(results, key=lambda r: r["current_price"])
    for r in sorted_results:
        candidate = r["candidate"]
        top_flight = r["top_flight"]
        evaluation = r["evaluation"]
        price = r["current_price"]
        airline = escape_markdown(top_flight["airline"])

        marker = ""
        if evaluation["hard_deal"]:
            marker = " 🎯"
        elif evaluation["median_deal"]:
            marker = f" 📉{evaluation['pct_drop']:.0f}%"

        duration_str = ""
        if top_flight.get("total_duration"):
            hours, minutes = divmod(top_flight["total_duration"], 60)
            duration_str = f", {hours}h{minutes}m"

        date_range = f"{candidate['outbound_date']} - {candidate['return_date']} ({candidate['nights']}n)"
        lines.append(f"• {date_range}: {price:,.0f} {currency} — {airline}{duration_str}{marker}")
        if top_flight.get("google_flights_url") and (evaluation["is_deal"] or r is sorted_results[0]):
            lines.append(f"  [View]({top_flight['google_flights_url']})")

    if any(r["evaluation"]["median_price"] is not None for r in results):
        median_price = next(r["evaluation"]["median_price"] for r in results if r["evaluation"]["median_price"] is not None)
        lines.append("")
        lines.append(f"📊 Historical median for this window: {median_price:,.0f} {currency}")

    window_best_line = format_window_best_line(window_best, currency)
    if window_best_line:
        lines.append("")
        lines.append(window_best_line)

    return "\n".join(lines)


def send_telegram_alert(message: str, bot_token: str, chat_id: str) -> bool:
    url = f"{TELEGRAM_API_BASE}/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": False,
    }
    try:
        response = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as exc:
        log.error("Failed to send Telegram alert: %s", exc)
        return False


# --------------------------------------------------------------------------
# Git sync (persist price_history.json back to the repo)
# --------------------------------------------------------------------------

def commit_and_push_history() -> None:
    """Stage, commit, and push price_history.json if it changed. Expects to
    run inside a git checkout with push credentials already configured
    (e.g. actions/checkout with persist-credentials: true and a token that
    has contents: write permission)."""

    try:
        status = subprocess.run(
            ["git", "status", "--porcelain", str(HISTORY_FILE)],
            capture_output=True,
            text=True,
            check=True,
        )
        if not status.stdout.strip():
            log.info("No changes to %s, skipping commit.", HISTORY_FILE)
            return

        subprocess.run(
            ["git", "config", "user.name", "flight-deals-tracker-bot"], check=True
        )
        subprocess.run(
            ["git", "config", "user.email", "actions@users.noreply.github.com"],
            check=True,
        )
        subprocess.run(["git", "add", str(HISTORY_FILE)], check=True)
        subprocess.run(
            [
                "git",
                "commit",
                "-m",
                f"chore: update price history ({datetime.now(timezone.utc).isoformat()})",
            ],
            check=True,
        )
        subprocess.run(["git", "push"], check=True)
        log.info("Pushed updated %s to the repository.", HISTORY_FILE)

    except subprocess.CalledProcessError as exc:
        log.error("Git sync failed: %s", exc)
    except FileNotFoundError:
        log.warning("git binary not found; skipping repository sync.")


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def load_routes() -> list[dict]:
    if not ROUTES_FILE.exists():
        log.error("Routes file %s not found.", ROUTES_FILE)
        return []
    try:
        with ROUTES_FILE.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        log.error("Failed to read %s: %s", ROUTES_FILE, exc)
        return []


def main() -> int:
    serpapi_key = (os.environ.get("SERPAPI_API_KEY") or "").strip()
    telegram_token = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
    telegram_chat_id = (os.environ.get("TELEGRAM_CHAT_ID") or "").strip()

    missing = [
        name
        for name, val in [
            ("SERPAPI_API_KEY", serpapi_key),
            ("TELEGRAM_BOT_TOKEN", telegram_token),
            ("TELEGRAM_CHAT_ID", telegram_chat_id),
        ]
        if not val
    ]
    if missing:
        log.error("Missing required environment variables: %s", ", ".join(missing))
        return 1

    routes = load_routes()
    if not routes:
        log.error("No routes configured, nothing to do.")
        return 1

    history = load_price_history()
    messages_sent = 0

    for route in routes:
        label = route_label(route)
        candidates = generate_date_candidates(route)
        if not candidates:
            log.warning("Skipping %s: no valid date/night candidates in window.", label)
            continue

        wkey = window_key(route)
        window_entry = history.setdefault(
            wkey,
            {
                "route_label": label,
                "currency": route.get("currency", "MYR"),
                "prices": [],
                "best": None,
                "cursor": 0,
            },
        )
        cursor = window_entry.get("cursor", 0) % len(candidates)
        batch_size = max(1, min(int(route.get("combos_per_run", DEFAULT_COMBOS_PER_RUN)), len(candidates)))
        batch = [candidates[(cursor + i) % len(candidates)] for i in range(batch_size)]
        window_entry["cursor"] = (cursor + batch_size) % len(candidates)

        log.info(
            "Checking route %s: %d candidates in window, %d this run.",
            label,
            len(candidates),
            batch_size,
        )

        results = []
        window_best = None
        for candidate in batch:
            candidate_label = (
                f"{candidate['outbound_date']} - {candidate['return_date']} ({candidate['nights']}n)"
            )
            try:
                leg = dict(route)
                leg["outbound_date"] = candidate["outbound_date"]
                leg["return_date"] = candidate["return_date"]

                data = fetch_flight_data(leg, serpapi_key)
                if not data:
                    log.warning("Skipping %s %s: no data returned.", label, candidate_label)
                    continue

                top_flights = extract_top_flights(data)
                if not top_flights:
                    log.warning("Skipping %s %s: no priced flights in response.", label, candidate_label)
                    continue

                current_price = top_flights[0]["price"]
                evaluation = evaluate_deal(leg, current_price, history.get(wkey))

                log.info(
                    "%s %s: current lowest price %.0f %s (median deal=%s, hard deal=%s)",
                    label,
                    candidate_label,
                    current_price,
                    route.get("currency", "MYR"),
                    evaluation["median_deal"],
                    evaluation["hard_deal"],
                )

                updated_entry = update_history_entry(history, route, candidate, current_price)
                window_best = updated_entry.get("best")
                results.append(
                    {
                        "candidate": candidate,
                        "current_price": current_price,
                        "top_flight": top_flights[0],
                        "evaluation": evaluation,
                    }
                )

            except Exception:
                log.exception("Unexpected error processing %s %s, continuing.", label, candidate_label)
                continue

        if results:
            message = format_batch_message(route, results, window_best)
            if send_telegram_alert(message, telegram_token, telegram_chat_id):
                messages_sent += 1
        else:
            log.warning("Skipping Telegram message for %s: no combos returned data this run.", label)

    save_price_history(history)
    log.info("Run complete. %s message(s) sent.", messages_sent)

    if os.environ.get("GITHUB_ACTIONS") == "true":
        commit_and_push_history()

    return 0


if __name__ == "__main__":
    sys.exit(main())
