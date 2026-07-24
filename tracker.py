#!/usr/bin/env python3
"""
Flight price tracker: pulls Google Flights data from SerpApi, compares it
against locally persisted history, and alerts on Telegram when a route gets
cheap. Designed to run headless on a GitHub Actions cron schedule.
"""

import itertools
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


def fetch_itinerary_price(route: dict, candidate: dict, serpapi_key: str) -> dict | None:
    """Price a multi-city candidate as separate one-way tickets, one per leg,
    and sum them -- see the multi-city design note in routes.json. Returns a
    top_flight-shaped dict (same shape as extract_top_flights() entries) with
    an extra "legs" breakdown, or None if any leg fails to return a priced
    flight (an itinerary can't be half-priced)."""
    legs_config = route["legs"]
    currency = route.get("currency", "MYR")

    priced_legs = []
    for leg, date in zip(legs_config, candidate["leg_dates"]):
        leg_route = {
            "departure_id": leg["from"],
            "arrival_id": leg["to"],
            "outbound_date": date,
            "currency": currency,
        }
        data = fetch_flight_data(leg_route, serpapi_key)
        if not data:
            return None
        top_flights = extract_top_flights(data)
        if not top_flights:
            return None
        cheapest = top_flights[0]
        priced_legs.append(
            {
                "from": leg["from"],
                "to": leg["to"],
                "date": date,
                "price": cheapest["price"],
                "airline": cheapest["airline"],
                "total_duration": cheapest.get("total_duration"),
                "google_flights_url": cheapest.get("google_flights_url"),
            }
        )

    durations = [l["total_duration"] for l in priced_legs if l.get("total_duration")]
    return {
        "price": sum(l["price"] for l in priced_legs),
        "airline": None,
        "total_duration": sum(durations) if durations else None,
        "google_flights_url": None,
        "legs": priced_legs,
    }


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


def is_multi_city(route: dict) -> bool:
    return "legs" in route


def multi_city_route_label(route: dict) -> str:
    legs = route["legs"]
    cities = [legs[0]["from"]] + [leg["to"] for leg in legs]
    return " -> ".join(cities)


def multi_city_window_key(route: dict) -> str:
    """Mirrors window_key() but folds in the full leg chain, so the pooled
    history/cursor resets if the city sequence or any leg's nights range
    changes."""
    legs_sig = "|".join(
        f"{leg['from']}>{leg['to']}:{leg.get('min_nights', '')}-{leg.get('max_nights', '')}"
        for leg in route["legs"]
    )
    return "-".join(
        [
            "multi_city",
            legs_sig,
            route.get("currency", "MYR"),
            route["earliest_departure"],
            route["latest_departure"],
        ]
    )


def format_candidate_label(route: dict, candidate: dict) -> str:
    """Human-readable description of a candidate combo, used for logging and
    as the persisted label for a window's best-seen combo."""
    if not is_multi_city(route):
        return f"{candidate['outbound_date']} - {candidate['return_date']} ({candidate['nights']}n)"

    legs = route["legs"]
    leg_dates = candidate["leg_dates"]
    nights = candidate["nights"]
    parts = []
    for i, leg in enumerate(legs):
        parts.append(f"{leg['from']}->{leg['to']} {leg_dates[i]}")
        if i < len(nights):
            parts.append(f"(stay {nights[i]}n)")
    return " ".join(parts)


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


def generate_multi_city_candidates(route: dict) -> list[dict]:
    """Expand a multi-city route's leg-0 departure window + each
    intermediate leg's stay-nights range into every combo. Each candidate
    is {"leg_dates": [<departure date per leg>], "nights": [<stay nights
    after each non-final leg>]}, sorted by leg-0 departure date."""
    legs = route.get("legs") or []
    label = multi_city_route_label(route) if len(legs) >= 2 else "multi-city route"

    if len(legs) < 2:
        log.error("Multi-city route %s needs at least 2 legs.", label)
        return []

    try:
        earliest = datetime.strptime(route["earliest_departure"], "%Y-%m-%d").date()
        latest = datetime.strptime(route["latest_departure"], "%Y-%m-%d").date()
        nights_ranges = [
            range(int(leg["min_nights"]), int(leg["max_nights"]) + 1)
            for leg in legs[:-1]
        ]
    except (KeyError, ValueError) as exc:
        log.error("Invalid leg/date config for route %s: %s", label, exc)
        return []

    if earliest > latest or any(r.start > r.stop - 1 or r.start < 1 for r in nights_ranges):
        log.error("Invalid date window or nights bounds for route %s.", label)
        return []

    candidates = []
    day_count = (latest - earliest).days + 1
    for offset in range(day_count):
        departure = earliest + timedelta(days=offset)
        for nights_combo in itertools.product(*nights_ranges):
            leg_dates = [departure]
            for nights in nights_combo:
                leg_dates.append(leg_dates[-1] + timedelta(days=nights))
            candidates.append(
                {
                    "leg_dates": [d.isoformat() for d in leg_dates],
                    "nights": list(nights_combo),
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

    hard_threshold = route.get("hard_threshold", HARD_THRESHOLD_MYR)
    hard_deal = currency == "MYR" and current_price <= hard_threshold

    return {
        "is_deal": median_deal or hard_deal,
        "median_deal": median_deal,
        "hard_deal": hard_deal,
        "median_price": median_price,
        "pct_drop": pct_drop,
    }


def update_history_entry(
    history: dict, route: dict, candidate: dict, current_price: float, top_flight: dict
) -> dict:
    """Append this observation to the window's pooled price history and
    update the best combo seen so far. Returns the updated entry."""
    key = multi_city_window_key(route) if is_multi_city(route) else window_key(route)
    label = multi_city_route_label(route) if is_multi_city(route) else route_label(route)
    entry = history.setdefault(
        key,
        {
            "route_label": label,
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
            "label": format_candidate_label(route, candidate),
            "airline": top_flight.get("airline"),
            "total_duration": top_flight.get("total_duration"),
            "legs": top_flight.get("legs"),
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


def format_duration(total_duration: int | None) -> str:
    if not total_duration:
        return ""
    hours, minutes = divmod(total_duration, 60)
    return f", {hours}h{minutes}m"


def format_leg_line(leg: dict, currency: str, show_link: bool) -> list[str]:
    airline = escape_markdown(leg.get("airline") or "Unknown airline")
    lines = [
        f"    {leg['from']}->{leg['to']} ({leg['date']}): "
        f"{leg['price']:,.0f} {currency} — {airline}{format_duration(leg.get('total_duration'))}"
    ]
    if show_link and leg.get("google_flights_url"):
        lines.append(f"      [View]({leg['google_flights_url']})")
    return lines


def format_window_best_line(window_best: dict | None, currency: str) -> str | None:
    if not window_best:
        return None

    label = window_best.get("label")
    if label is None:
        # Backward-compat: entries persisted before the "label" field existed.
        label = (
            f"{window_best['outbound_date']} - {window_best['return_date']} "
            f"({window_best['nights']}n)"
        )

    if window_best.get("legs"):
        lines = [f"🏆 Best in window so far: {window_best['price']:,.0f} {currency} total on {label}"]
        for leg in window_best["legs"]:
            lines.extend(format_leg_line(leg, currency, show_link=False))
        return "\n".join(lines)

    airline_str = f" — {window_best['airline']}" if window_best.get("airline") else ""
    return (
        f"🏆 Best in window so far: {window_best['price']:,.0f} {currency} "
        f"on {label}{airline_str}{format_duration(window_best.get('total_duration'))}"
    )


def format_batch_message(route: dict, results: list[dict], window_best: dict | None) -> str:
    """Build one consolidated Telegram message covering every date/night
    combo checked this run for a route. `results` is a list of dicts with
    keys: candidate, current_price, top_flight, evaluation (in the order
    they were checked)."""
    route_label_str = multi_city_route_label(route) if is_multi_city(route) else route_label(route)
    label = escape_markdown(route_label_str)
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

        marker = ""
        if evaluation["hard_deal"]:
            marker = " 🎯"
        elif evaluation["median_deal"]:
            marker = f" 📉{evaluation['pct_drop']:.0f}%"

        date_range = format_candidate_label(route, candidate)
        show_link = evaluation["is_deal"] or r is sorted_results[0]

        if top_flight.get("legs"):
            lines.append(f"• {date_range}: {price:,.0f} {currency} total{marker}")
            for leg in top_flight["legs"]:
                lines.extend(format_leg_line(leg, currency, show_link=show_link))
        else:
            airline = escape_markdown(top_flight["airline"])
            lines.append(
                f"• {date_range}: {price:,.0f} {currency} — {airline}"
                f"{format_duration(top_flight.get('total_duration'))}{marker}"
            )
            if show_link and top_flight.get("google_flights_url"):
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
        multi_city = is_multi_city(route)
        label = multi_city_route_label(route) if multi_city else route_label(route)
        candidates = generate_multi_city_candidates(route) if multi_city else generate_date_candidates(route)
        if not candidates:
            log.warning("Skipping %s: no valid date/night candidates in window.", label)
            continue

        wkey = multi_city_window_key(route) if multi_city else window_key(route)
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
            candidate_label = format_candidate_label(route, candidate)
            try:
                if multi_city:
                    top_flight = fetch_itinerary_price(route, candidate, serpapi_key)
                    if not top_flight:
                        log.warning("Skipping %s %s: a leg returned no priced flight.", label, candidate_label)
                        continue
                    evaluation_route = route
                else:
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
                    top_flight = top_flights[0]
                    evaluation_route = leg

                current_price = top_flight["price"]
                evaluation = evaluate_deal(evaluation_route, current_price, history.get(wkey))

                log.info(
                    "%s %s: current lowest price %.0f %s (median deal=%s, hard deal=%s)",
                    label,
                    candidate_label,
                    current_price,
                    route.get("currency", "MYR"),
                    evaluation["median_deal"],
                    evaluation["hard_deal"],
                )

                updated_entry = update_history_entry(
                    history, route, candidate, current_price, top_flight
                )
                window_best = updated_entry.get("best")
                results.append(
                    {
                        "candidate": candidate,
                        "current_price": current_price,
                        "top_flight": top_flight,
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
