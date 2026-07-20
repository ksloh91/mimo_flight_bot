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
from datetime import datetime, timezone
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
MAX_HISTORY_ENTRIES_PER_ROUTE = 30
MIN_HISTORY_FOR_MEDIAN_CHECK = 3

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


def route_key(route: dict) -> str:
    return "-".join(
        [
            route["departure_id"],
            route["arrival_id"],
            route["outbound_date"],
            route.get("return_date") or "oneway",
            route.get("currency", "MYR"),
        ]
    )


def route_label(route: dict) -> str:
    return f"{route['departure_id']} -> {route['arrival_id']}"


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


def update_history_entry(history: dict, route: dict, current_price: float) -> None:
    key = route_key(route)
    entry = history.setdefault(
        key, {"route_label": route_label(route), "currency": route.get("currency", "MYR"), "prices": []}
    )
    entry["prices"].append(current_price)
    entry["prices"] = entry["prices"][-MAX_HISTORY_ENTRIES_PER_ROUTE:]
    entry["last_updated"] = datetime.now(timezone.utc).isoformat()


# --------------------------------------------------------------------------
# Telegram alerting
# --------------------------------------------------------------------------

def escape_markdown(text: str) -> str:
    """Escape special characters for Telegram's legacy Markdown parse mode."""
    for char in ("_", "*", "[", "`"):
        text = text.replace(char, f"\\{char}")
    return text


def format_alert_message(route: dict, top_flights: list[dict], evaluation: dict) -> str:
    cheapest = top_flights[0]
    label = escape_markdown(route_label(route))
    airline = escape_markdown(cheapest["airline"])
    price = cheapest["price"]
    currency = route.get("currency", "MYR")

    lines = ["🚨 *DEAL ALERT!* 🚨", "", f"✈️ *Route:* {label}", f"💰 *Price:* {price:,.0f} {currency}"]

    if evaluation["hard_deal"]:
        lines.append(f"🎯 Below your hard threshold of {HARD_THRESHOLD_MYR:,.0f} {currency}")
    if evaluation["median_deal"]:
        lines.append(
            f"📉 {evaluation['pct_drop']:.1f}% below the historical median "
            f"({evaluation['median_price']:,.0f} {currency})"
        )

    lines.append(f"🛫 *Airline:* {airline}")
    if cheapest.get("total_duration"):
        hours, minutes = divmod(cheapest["total_duration"], 60)
        lines.append(f"⏱️ *Duration:* {hours}h {minutes}m")

    lines.append(f"📅 *Dates:* {route['outbound_date']}" + (f" - {route['return_date']}" if route.get("return_date") else ""))

    if len(top_flights) > 1:
        lines.append("")
        lines.append("*Other cheap options:*")
        for flight in top_flights[1:]:
            lines.append(f"  • {flight['price']:,.0f} {currency} — {escape_markdown(flight['airline'])}")

    if cheapest.get("google_flights_url"):
        lines.append("")
        lines.append(f"[View on Google Flights]({cheapest['google_flights_url']})")

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
    serpapi_key = os.environ.get("SERPAPI_API_KEY")
    telegram_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    telegram_chat_id = os.environ.get("TELEGRAM_CHAT_ID")

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
    alerts_sent = 0

    for route in routes:
        label = route_label(route)
        log.info("Checking route %s (%s)", label, route["outbound_date"])

        try:
            data = fetch_flight_data(route, serpapi_key)
            if not data:
                log.warning("Skipping %s: no data returned.", label)
                continue

            top_flights = extract_top_flights(data)
            if not top_flights:
                log.warning("Skipping %s: no priced flights in response.", label)
                continue

            current_price = top_flights[0]["price"]
            key = route_key(route)
            evaluation = evaluate_deal(route, current_price, history.get(key))

            log.info(
                "%s: current lowest price %.0f %s (median deal=%s, hard deal=%s)",
                label,
                current_price,
                route.get("currency", "MYR"),
                evaluation["median_deal"],
                evaluation["hard_deal"],
            )

            if evaluation["is_deal"]:
                message = format_alert_message(route, top_flights, evaluation)
                if send_telegram_alert(message, telegram_token, telegram_chat_id):
                    alerts_sent += 1

            update_history_entry(history, route, current_price)

        except Exception:
            log.exception("Unexpected error processing route %s, continuing.", label)
            continue

    save_price_history(history)
    log.info("Run complete. %s alert(s) sent.", alerts_sent)

    if os.environ.get("GITHUB_ACTIONS") == "true":
        commit_and_push_history()

    return 0


if __name__ == "__main__":
    sys.exit(main())
