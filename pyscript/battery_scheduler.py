from datetime import datetime, timedelta, timezone

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(value, default=0.0):
    try:
        if value in [None, "", "unknown", "unavailable", "none", "None"]:
            return default
        return float(value)
    except Exception:
        return default


def _read_state(entity_id, default=None):
    try:
        val = state.get(entity_id)
        return default if val is None else val
    except Exception:
        return default


def _read_bool(entity_id, default=False):
    try:
        v = str(_read_state(entity_id, "on" if default else "off")).strip().lower()
        return v in ["on", "true", "1", "yes", "enabled"]
    except Exception:
        return default


def _set_input_number(entity_id, value):
    input_number.set_value(entity_id=entity_id, value=round(float(value), 4))


def _set_input_text(entity_id, value):
    input_text.set_value(entity_id=entity_id, value=str(value)[:255])


def _log_info(msg):
    log.info(f"[battery_scheduler] {msg}")


def _log_debug(msg):
    log.debug(f"[battery_scheduler] {msg}")


def _truncate(text, max_len=255):
    return str(text)[:max_len]


def _current_slot_iso(now_dt):
    minute = (now_dt.minute // 15) * 15
    return now_dt.replace(minute=minute, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%S")


def _parse_compact_prices(raw):
    if not raw or ";" not in raw:
        return []
    try:
        start_str, prices_str = raw.split(";", 1)
        start_dt = datetime.strptime(start_str, "%Y-%m-%dT%H:%M")
    except Exception:
        return []
    out = []
    tokens = [t.strip() for t in prices_str.split(",") if t.strip()]
    for idx, token in enumerate(tokens):
        try:
            p = int(token) / 10000.0
            slot_dt = start_dt + timedelta(minutes=15 * idx)
            out.append({"start_time": slot_dt.strftime("%Y-%m-%dT%H:%M:%S"), "price": p})
        except Exception:
            continue
    return out


def _future_slots_only(prices, current_slot):
    return [s for s in prices if s["start_time"][:19] >= current_slot]


def _compact_slot_strings(times):
    return ",".join([f"{t[5:7]}{t[8:10]}{t[11:13]}{t[14:16]}" for t in times])[:255]


def _detail_slot_strings(slots, today_str):
    result = []
    overflow = 0
    total_len = 0
    for slot in sorted(slots, key=lambda x: x["start_time"]):
        is_tomorrow = slot["start_time"][:10] != today_str
        t = f"{'*' if is_tomorrow else ''}{slot['start_time'][11:13]}{slot['start_time'][14:16]}"
        entry = f"{t}|{round(slot['price'] * 100, 1)}"
        needed = len(entry) + (1 if result else 0)
        if total_len + needed <= 245:
            result.append(entry)
            total_len += needed
        else:
            overflow += 1
    out = ",".join(result)
    if overflow > 0:
        out += f",+{overflow}"
    return out[:255]


def _to_u(kwh, soc_unit):
    return int(round(float(kwh) / float(soc_unit)))


def _percentile(sorted_values, fraction):
    if not sorted_values:
        return 0.0
    idx = max(0, min(len(sorted_values) - 1, int(len(sorted_values) * fraction)))
    return sorted_values[idx]


# ---------------------------------------------------------------------------
# Time-of-day load profile (96 quarters: q = hour*4 + minute//15)
# Stored as two fixed-width strings (48 quarters * 4 digits = 192 chars each):
#   input_text.battery_load_profile_am  (00:00 .. 11:45)
#   input_text.battery_load_profile_pm  (12:00 .. 23:45)
# ---------------------------------------------------------------------------

def _parse_half_profile(raw, n=48):
    if not raw or len(raw) < n * 4:
        return [0] * n
    try:
        return [int(raw[i * 4:(i + 1) * 4]) for i in range(n)]
    except Exception:
        return [0] * n


def _parse_load_profile(raw_am, raw_pm):
    return _parse_half_profile(raw_am, 48) + _parse_half_profile(raw_pm, 48)


def _format_load_profile(arr):
    safe = [max(0, min(9999, int(round(w)))) for w in arr[:96]]
    am = "".join(f"{w:04d}" for w in safe[:48])
    pm = "".join(f"{w:04d}" for w in safe[48:96])
    return am, pm


def _quarter_of_day(iso_ts):
    # iso_ts like "2026-04-22T13:15:00"
    h = int(iso_ts[11:13])
    m = int(iso_ts[14:16])
    return h * 4 + (m // 15)


# ---------------------------------------------------------------------------
# DP core
# ---------------------------------------------------------------------------

def _dp_optimize(
    prices,
    initial_soc_kwh,
    capacity_kwh,
    max_charge_slot_kwh,
    charge_gain_kwh,
    charge_efficiency_to_battery,
    discharge_batt_kwh_by_slot,
    discharge_delivery_kwh_by_slot,
    discharge_efficiency_from_battery,
    min_soc_floor_kwh,
):
    """
    DP over future 15-min slots with per-slot discharge rate.
    - charge cost uses actual grid kWh drawn (max_charge_slot_kwh / charge_efficiency)
    - discharge battery-side kWh + delivered kWh vary per slot (from time-of-day load profile)
    - terminal value at 20th-percentile price for residual SOC above floor
    """
    N = len(prices)
    if N == 0:
        return [], 0.0

    SOC_UNIT = 0.01

    sorted_prices = sorted([float(p["price"]) for p in prices])
    terminal_price = _percentile(sorted_prices, 0.20)

    capacity_u = _to_u(capacity_kwh, SOC_UNIT)
    charge_gain_u = max(_to_u(charge_gain_kwh, SOC_UNIT), 1)
    discharge_u_by_slot = [max(_to_u(k, SOC_UNIT), 1) for k in discharge_batt_kwh_by_slot]
    init_u = max(0, min(capacity_u, _to_u(initial_soc_kwh, SOC_UNIT)))
    floor_u = _to_u(min_soc_floor_kwh, SOC_UNIT)

    NSTATES = capacity_u + 1

    terminal_value_per_u = SOC_UNIT * float(discharge_efficiency_from_battery) * terminal_price
    V = [max(0, s - floor_u) * terminal_value_per_u for s in range(NSTATES)]
    policy_stack = []

    avg_d_u = sum(discharge_u_by_slot) / max(1, len(discharge_u_by_slot))
    _log_info(
        f"dp N={N} cap_u={capacity_u} states={NSTATES} "
        f"terminal_price={terminal_price:.4f} "
        f"terminal_val_per_u={terminal_value_per_u:.5f} "
        f"avg_discharge_u={avg_d_u:.2f}"
    )

    # DP maximizes RAW economic value; margin/spread/value checks are
    # applied post-hoc in _build_schedule's `profitable` gate.
    for t in range(N - 1, -1, -1):
        price = float(prices[t]["price"])
        c_cost = (max_charge_slot_kwh / charge_efficiency_to_battery) * price
        d_rev = discharge_delivery_kwh_by_slot[t] * price
        d_u = discharge_u_by_slot[t]

        new_V = [0.0] * NSTATES
        new_pol = [0] * NSTATES

        for s in range(NSTATES):
            best = V[s]
            best_a = 0

            if s < capacity_u:
                sc = min(s + charge_gain_u, capacity_u)
                vc = V[sc] - c_cost
                if vc > best:
                    best, best_a = vc, 1

            if s - d_u >= floor_u:
                sd = s - d_u
                vd = V[sd] + d_rev
                if vd > best:
                    best, best_a = vd, 2

            new_V[s] = best
            new_pol[s] = best_a

        V = new_V
        policy_stack.append(new_pol)

    policy_stack.reverse()

    s = init_u
    actions = []
    for t in range(N):
        act = policy_stack[t][s]
        actions.append(act)
        if act == 1:
            s = min(s + charge_gain_u, capacity_u)
        elif act == 2:
            s = max(s - discharge_u_by_slot[t], 0)

    # cycle_value = pure charge/discharge cashflows (no terminal). Used to
    # compute the per-kWh margin so it isn't inflated by leftover SOC value.
    cycle_value = 0.0
    for t in range(N):
        if actions[t] == 1:
            cycle_value -= (max_charge_slot_kwh / charge_efficiency_to_battery) * float(prices[t]["price"])
        elif actions[t] == 2:
            cycle_value += discharge_delivery_kwh_by_slot[t] * float(prices[t]["price"])

    # total_value adds terminal value of SOC above the floor so the
    # profitability gate sees the same number the DP optimized for.
    terminal_above_floor_kwh = max(0, s - floor_u) * SOC_UNIT
    terminal_value = terminal_above_floor_kwh * float(discharge_efficiency_from_battery) * terminal_price
    total_value = cycle_value + terminal_value

    return actions, cycle_value, total_value


# ---------------------------------------------------------------------------
# Schedule builder
# ---------------------------------------------------------------------------

def _build_schedule(
    prices,
    soc,
    capacity_kwh,
    max_charge_slot_kwh,
    discharge_batt_kwh_by_slot,
    discharge_delivery_kwh_by_slot,
    charge_efficiency_to_battery,
    discharge_efficiency_from_battery,
    min_profit,
    safety_margin,
    min_total_value,
    min_price_spread,
    min_discharge_soc,
):
    required_margin = float(min_profit) + float(safety_margin)
    start_energy_kwh = max(
        0.0, min(float(capacity_kwh), (float(soc) / 100.0) * float(capacity_kwh))
    )
    # max_charge_slot_kwh is BATTERY-side energy added per slot (consistent
    # with discharge convention). Grid-side draw = that / charge_efficiency,
    # which is what the DP uses for cost.
    charge_gain_kwh = float(max_charge_slot_kwh)
    min_soc_floor_kwh = float(capacity_kwh) * (float(min_discharge_soc) / 100.0)

    if not prices:
        _log_info("no prices available after parsing cache")
        return {
            "charge_slots": [], "discharge_slots": [],
            "expected_profit": 0.0, "total_expected_value": 0.0,
            "charge_ceiling": 0.0, "discharge_floor": 0.0,
            "price_spread": 0.0, "profitable": False,
        }

    actions, cycle_value, total_value = _dp_optimize(
        prices=prices,
        initial_soc_kwh=start_energy_kwh,
        capacity_kwh=float(capacity_kwh),
        max_charge_slot_kwh=float(max_charge_slot_kwh),
        charge_gain_kwh=charge_gain_kwh,
        charge_efficiency_to_battery=float(charge_efficiency_to_battery),
        discharge_batt_kwh_by_slot=discharge_batt_kwh_by_slot,
        discharge_delivery_kwh_by_slot=discharge_delivery_kwh_by_slot,
        discharge_efficiency_from_battery=float(discharge_efficiency_from_battery),
        min_soc_floor_kwh=min_soc_floor_kwh,
    )

    charge_slots = []
    discharge_slots = []
    total_discharge_delivery = 0.0
    for i, act in enumerate(actions):
        if act == 1:
            charge_slots.append({"start_time": prices[i]["start_time"], "price": prices[i]["price"]})
        elif act == 2:
            discharge_slots.append({"start_time": prices[i]["start_time"], "price": prices[i]["price"]})
            total_discharge_delivery += discharge_delivery_kwh_by_slot[i]

    n_discharge = len(discharge_slots)
    total_expected_value = total_value  # includes terminal SOC value (for gating)

    # Per-kWh margin from cycle cashflows only, so it's not inflated by
    # residual SOC value when the plan ends with energy still in the battery.
    expected_profit = (
        cycle_value / total_discharge_delivery
        if total_discharge_delivery > 0 else 0.0
    )

    charge_ceiling = max([s["price"] for s in charge_slots]) if charge_slots else 0.0
    min_charge_price = min([s["price"] for s in charge_slots]) if charge_slots else 0.0
    discharge_floor = min([s["price"] for s in discharge_slots]) if discharge_slots else 0.0

    all_prices_list = [s["price"] for s in prices]
    price_spread = (max(all_prices_list) - min(all_prices_list)) if all_prices_list else 0.0

    profitable = (
        n_discharge > 0
        and total_expected_value > float(min_total_value)
        and expected_profit > required_margin
        and price_spread >= float(min_price_spread)
    )

    _log_info(
        f"schedule: charge={len(charge_slots)} discharge={n_discharge} "
        f"total_value={total_expected_value:.4f} expected_profit={expected_profit:.4f} "
        f"price_spread={price_spread:.4f} profitable={profitable} "
        f"total_discharge_delivery={total_discharge_delivery:.4f}"
    )

    return {
        "charge_slots": charge_slots,
        "discharge_slots": discharge_slots,
        "expected_profit": round(expected_profit, 4),
        "total_expected_value": round(total_expected_value, 4),
        "charge_ceiling": round(charge_ceiling, 4),
        "min_charge_price": round(min_charge_price, 4),
        "discharge_floor": round(discharge_floor, 4),
        "price_spread": round(price_spread, 4),
        "profitable": profitable,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

@service
def battery_schedule_run():
    try:
        _set_input_text("input_text.battery_debug_stage", "start")
        _set_input_text("input_text.battery_debug_error", "")

        now_dt = datetime.now()
        today_str = now_dt.strftime("%Y-%m-%d")
        current_slot = _current_slot_iso(now_dt)
        _set_input_text("input_text.battery_debug_stage", f"time ok {current_slot}")

        soc = _safe_float(_read_state("sensor.plug_in_battery_state_of_charge", 50), 50)
        soc = min(100.0, max(0.0, soc))
        _set_input_text("input_text.battery_debug_stage", f"soc ok {soc}")

        min_profit = _safe_float(_read_state("input_number.battery_min_profit_threshold", 0.05), 0.05)
        safety_margin = _safe_float(_read_state("input_number.battery_schedule_safety_margin", 0.02), 0.02)
        min_total_value = _safe_float(_read_state("input_number.battery_schedule_min_total_value", 0.03), 0.03)
        min_discharge_soc = _safe_float(_read_state("input_number.battery_min_discharge_soc", 0), 0)

        capacity_kwh = _safe_float(_read_state("input_number.battery_capacity_kwh", 2.7), 2.7)
        max_charge_slot_kwh = _safe_float(_read_state("input_number.battery_max_charge_slot_kwh", 0.200), 0.200)
        max_discharge_slot_kwh = _safe_float(_read_state("input_number.battery_max_discharge_slot_kwh", 0.200), 0.200)

        # Derive realistic discharge cap from observed home load.
        # In HomeWizard 'zero' mode the battery is load-following: it can
        # never deliver more than the house is consuming. DP must plan on
        # the smaller of (battery rated rate, avg home load rate).
        avg_home_load_w = _safe_float(_read_state("input_number.battery_avg_home_load_w", 400), 400)

        # Time-of-day load profile (96 quarters). Each future price slot gets
        # its own realistic discharge rate from the bucket matching its
        # quarter-of-day. Never-sampled buckets fall back to avg_home_load_w.
        raw_am = str(_read_state("input_text.battery_load_profile_am", "") or "")
        raw_pm = str(_read_state("input_text.battery_load_profile_pm", "") or "")
        load_profile_w = _parse_load_profile(raw_am, raw_pm)

        charge_efficiency_to_battery = _safe_float(
            _read_state("input_number.battery_charge_efficiency", 0.75), 0.75
        )
        discharge_efficiency_from_battery = _safe_float(
            _read_state("input_number.battery_discharge_efficiency", 0.95), 0.95
        )
        min_price_spread = _safe_float(
            _read_state("input_number.battery_min_price_spread", 0.04), 0.04
        )

        opportunistic_threshold = _safe_float(
            _read_state("input_number.battery_opportunistic_charge_threshold", 0.14), 0.14
        )

        _set_input_text("input_text.battery_debug_stage", "inputs ok")

        raw = "".join([
            str(_read_state("input_text.battery_tibber_prices_cache_1", "") or ""),
            str(_read_state("input_text.battery_tibber_prices_cache_2", "") or ""),
            str(_read_state("input_text.battery_tibber_prices_cache_3", "") or ""),
            str(_read_state("input_text.battery_tibber_prices_cache_4", "") or ""),
            str(_read_state("input_text.battery_tibber_prices_cache_5", "") or ""),
        ])

        prices = _parse_compact_prices(raw)
        prices = _future_slots_only(prices, current_slot)
        _set_input_text("input_text.battery_debug_stage", f"prices ok {len(prices)}")

        live_price_sensor = _read_state("sensor.home54_electricity_price", None)
        live_price = _safe_float(live_price_sensor, None)
        if live_price is None:
            live_price = prices[0]["price"] if prices else 0.0
        _set_input_text("input_text.battery_debug_stage", f"live ok {live_price}")

        # Build per-slot discharge capacity from the time-of-day profile.
        # Each slot's cap = min(battery rated rate, profile[quarter] W).
        discharge_batt_kwh_by_slot = []
        discharge_delivery_kwh_by_slot = []
        for p in prices:
            q = _quarter_of_day(p["start_time"])
            w = load_profile_w[q] if 0 <= q < 96 else 0
            if w < 50:
                w = avg_home_load_w  # warmup fallback
            batt_kwh = min(max_discharge_slot_kwh, (w / 1000.0) * 0.25)
            discharge_batt_kwh_by_slot.append(batt_kwh)
            discharge_delivery_kwh_by_slot.append(batt_kwh * discharge_efficiency_from_battery)

        result = _build_schedule(
            prices=prices,
            soc=soc,
            capacity_kwh=capacity_kwh,
            max_charge_slot_kwh=max_charge_slot_kwh,
            discharge_batt_kwh_by_slot=discharge_batt_kwh_by_slot,
            discharge_delivery_kwh_by_slot=discharge_delivery_kwh_by_slot,
            charge_efficiency_to_battery=charge_efficiency_to_battery,
            discharge_efficiency_from_battery=discharge_efficiency_from_battery,
            min_profit=min_profit,
            safety_margin=safety_margin,
            min_total_value=min_total_value,
            min_price_spread=min_price_spread,
            min_discharge_soc=min_discharge_soc,
        )
        _set_input_text(
            "input_text.battery_debug_stage",
            f"build ok c={len(result['charge_slots'])} d={len(result['discharge_slots'])}"
        )

        charge_times = [s["start_time"] for s in result["charge_slots"]]
        discharge_times = [s["start_time"] for s in result["discharge_slots"]]
        has_tomorrow = any([slot["start_time"][:10] != today_str for slot in prices])

        _set_input_number("input_number.tibber_charge_ceiling", result["charge_ceiling"])
        _set_input_number("input_number.tibber_discharge_floor", result["discharge_floor"])
        _set_input_number("input_number.tibber_expected_profit", result["expected_profit"])

        _set_input_text("input_text.battery_charge_slots", _compact_slot_strings(charge_times))
        _set_input_text("input_text.battery_discharge_slots", _compact_slot_strings(discharge_times))
        _set_input_text("input_text.battery_charge_slots_detail", _detail_slot_strings(result["charge_slots"], today_str))
        _set_input_text("input_text.battery_discharge_slots_detail", _detail_slot_strings(result["discharge_slots"], today_str))

        profitable = bool(result["profitable"])

        # Use the current quarter's learned profile bucket for the discharge
        # gate so it matches what the DP planned for this specific slot.
        current_q = now_dt.hour * 4 + (now_dt.minute // 15)
        current_w = load_profile_w[current_q] if 0 <= current_q < 96 else avg_home_load_w
        if current_w < 50:
            current_w = avg_home_load_w
        current_discharge_slot_kwh = min(max_discharge_slot_kwh, (current_w / 1000.0) * 0.25)
        enough_energy_now = ((soc / 100.0) * capacity_kwh) >= (current_discharge_slot_kwh * 0.5)

        is_opportunistic = (
            opportunistic_threshold > 0
            and soc < 100
            and live_price >= 0
            and live_price <= opportunistic_threshold
            and (
                not result["charge_slots"]
                or live_price <= result["min_charge_price"]
            )
        )

        is_cheap = (
            ((live_price < 0) and (soc < 100))
            or ((soc < 100) and profitable and (current_slot in charge_times))
            or is_opportunistic
        )
        is_expensive = (
            profitable
            and (soc >= min_discharge_soc)
            and enough_energy_now
            and (current_slot in discharge_times)
        )

        # Discharge takes priority over opportunistic charge in the same slot
        if is_expensive:
            is_cheap = False

        if is_cheap:
            target_mode = "to_full"
        elif is_expensive:
            target_mode = "zero_discharge_only"
        else:
            target_mode = "standby"

        if not prices:
            reason = "no future prices"
        elif is_cheap and live_price < 0:
            reason = "negative price charge"
        elif is_cheap and is_opportunistic:
            reason = f"opportunistic charge {round(live_price * 100, 1)}ct < {round(opportunistic_threshold * 100, 1)}ct threshold"
        elif is_cheap:
            reason = "scheduled charge slot"
        elif is_expensive:
            reason = "scheduled discharge slot"
        elif not profitable:
            if result["total_expected_value"] <= min_total_value:
                reason = "schedule value too low"
            elif len(result["discharge_slots"]) == 0:
                reason = "no profitable discharge slots"
            else:
                reason = "profit below threshold"
        else:
            reason = "waiting for later slot"

        _set_input_text(
            "input_text.battery_debug_summary",
            _truncate(
                f"mode={target_mode} soc={soc:.1f}% "
                f"price={live_price*100:.1f}ct "
                f"prof={profitable} c={len(charge_times)} d={len(discharge_times)}"
            ),
        )
        _set_input_text("input_text.battery_debug_reason", _truncate(reason))
        _set_input_text("input_text.battery_debug_slot_now", current_slot)
        _set_input_text("input_text.battery_debug_charge_times", _truncate(",".join(charge_times)))
        _set_input_text("input_text.battery_debug_discharge_times", _truncate(",".join(discharge_times)))

        _set_input_number("input_number.battery_debug_total_value", result["total_expected_value"])
        _set_input_number("input_number.battery_debug_price_spread", result["price_spread"])
        _set_input_number("input_number.battery_debug_live_price", live_price)
        _set_input_number("input_number.battery_debug_soc", soc)
        _set_input_number("input_number.battery_debug_expected_profit", result["expected_profit"])
        _set_input_number("input_number.battery_debug_charge_count", len(charge_times))
        _set_input_number("input_number.battery_debug_discharge_count", len(discharge_times))

        _log_info(
            f"run current_slot={current_slot} target_mode={target_mode} "
            f"reason={reason} soc={soc:.1f} live_price={live_price:.4f} "
            f"opportunistic_threshold={opportunistic_threshold:.4f} "
            f"profitable={profitable} charge_count={len(charge_times)} "
            f"discharge_count={len(discharge_times)}"
        )
        _log_debug(f"charge_times={charge_times} discharge_times={discharge_times}")

        last_mode = str(_read_state("input_text.battery_last_mode", "unknown") or "unknown")
        mode_changed = target_mode != last_mode

        select.select_option(
            entity_id="select.p1_meter_battery_group_mode",
            option=target_mode,
        )
        _set_input_text("input_text.battery_last_mode", target_mode)

        if mode_changed:
            if target_mode == "to_full":
                script.battery_notify(
                    title="🤑 Battery → Charging (NEGATIVE)" if live_price < 0 else "🔋 Battery → Charging",
                    message=(
                        f"Price: {round(live_price * 100, 1)} ct\n"
                        f"SOC: {int(soc)}%\n"
                        f"Slots: {len(charge_times)} charge / {len(discharge_times)} discharge\n"
                        f"Expected margin: {round(result['expected_profit'] * 100, 2)} ct/kWh\n"
                        f"Expected schedule value: €{round(result['total_expected_value'], 3)}\n"
                        f"{'📅 Cross-day' if has_tomorrow else '📅 Today only'}"
                    ),
                )
            elif target_mode == "zero_discharge_only":
                script.battery_notify(
                    title="⚡ Battery → Discharging",
                    message=(
                        f"Price: {round(live_price * 100, 1)} ct\n"
                        f"SOC: {int(soc)}%\n"
                        f"Slots: {len(charge_times)} charge / {len(discharge_times)} discharge\n"
                        f"Expected margin: {round(result['expected_profit'] * 100, 2)} ct/kWh\n"
                        f"Expected schedule value: €{round(result['total_expected_value'], 3)}\n"
                        f"{'📅 Cross-day' if has_tomorrow else '📅 Today only'}"
                    ),
                )
            elif last_mode in ["to_full", "zero", "zero_discharge_only"]:
                script.battery_notify(
                    title="🔄 Battery → Hold",
                    message=(
                        "No profitable schedule found\n" if not profitable
                        else "Waiting for scheduled slot\n"
                    ) + (
                        f"SOC: {int(soc)}% · Slots: {len(charge_times)} charge / {len(discharge_times)} discharge"
                    ),
                )

        _set_input_text("input_text.battery_debug_stage", "done")

    except Exception as e:
        _set_input_text("input_text.battery_debug_error", str(e))
        _set_input_text("input_text.battery_debug_stage", "crashed")
        raise


# ---------------------------------------------------------------------------
# Load profile sampler
# ---------------------------------------------------------------------------

@service
def battery_sample_load():
    """
    Samples home load and updates:
      - the 96-step time-of-day profile bucket (EMA a=0.1)
      - the scalar battery_avg_home_load_w (EMA a=0.1) as a warmup fallback

    Sources depending on battery mode (solar production is always added so we
    capture true home consumption, not just net grid draw):
      - standby (idle):          home load = p1_meter_power + solar_W
      - zero (discharging):      home load = p1_meter_power + |battery_power| + solar_W
      - to_full (charging):      skipped (p1 mixes home load with grid charge)
    The 30 < load < 1500 W filter rejects export-dominated samples and
    EV-charging / heat-pump spikes.
    """
    try:
        if not _read_bool("input_boolean.battery_smart_control_enabled", True):
            return
        mode = str(_read_state("input_text.battery_last_mode", "") or "")

        # Solar production in W (sensor reports kW). The Envoy updates only
        # every few minutes, so accept the most recent value as long as it's
        # fresh within a reasonable window. If the sensor is stale or
        # unavailable, assume 0 W (safe underestimate of home load).
        solar_w = 0.0
        solar_raw = _read_state("sensor.envoy_122041077462_current_power_production", None)
        solar_kw = _safe_float(solar_raw, None)
        if solar_kw is not None:
            # Check staleness via last_updated attribute; skip if > 15 min old
            try:
                lu = state.get("sensor.envoy_122041077462_current_power_production.last_updated")
                if lu is not None:
                    # lu is a tz-aware datetime (UTC) from pyscript state metadata
                    age = (datetime.now(timezone.utc) - lu).total_seconds()
                    if age <= 900:
                        solar_w = max(0.0, solar_kw * 1000.0)
                else:
                    solar_w = max(0.0, solar_kw * 1000.0)
            except Exception:
                solar_w = max(0.0, solar_kw * 1000.0)

        load_w = None
        if mode in ("standby", "zero_charge_only"):
            p1 = _safe_float(_read_state("sensor.p1_meter_power", None), None)
            if p1 is None:
                return
            # home_load = net_grid + solar (p1>0 import, p1<0 export)
            w = p1 + solar_w
            if w <= 30 or w >= 1500:
                return
            load_w = w
        elif mode in ("zero", "zero_discharge_only"):
            bp = _safe_float(_read_state("sensor.plug_in_battery_power", None), None)
            if bp is None:
                return
            # In zero mode the battery is load-following. True home load =
            # net grid import (p1, positive = import) + battery discharge + solar.
            # p1 can be slightly negative if the battery momentarily over-delivers.
            p1 = _safe_float(_read_state("sensor.p1_meter_power", None), 0.0)
            w = p1 + abs(bp) + solar_w
            if w <= 30 or w >= 1500:
                return
            load_w = w
        else:
            return

        now_dt = datetime.now()
        q = now_dt.hour * 4 + (now_dt.minute // 15)

        raw_am = str(_read_state("input_text.battery_load_profile_am", "") or "")
        raw_pm = str(_read_state("input_text.battery_load_profile_pm", "") or "")
        profile = _parse_load_profile(raw_am, raw_pm)

        alpha = 0.1
        current = profile[q] if 0 <= q < 96 else 0
        if current <= 0:
            new_val = int(round(load_w))
        else:
            new_val = int(round(current + alpha * (load_w - current)))
        profile[q] = new_val

        am, pm = _format_load_profile(profile)
        _set_input_text("input_text.battery_load_profile_am", am)
        _set_input_text("input_text.battery_load_profile_pm", pm)

        # Scalar EMA fallback
        avg = _safe_float(_read_state("input_number.battery_avg_home_load_w", 400), 400)
        new_avg = (1 - alpha) * avg + alpha * load_w
        new_avg = max(50.0, min(3000.0, new_avg))
        _set_input_number("input_number.battery_avg_home_load_w", new_avg)

    except Exception as e:
        _log_info(f"sample_load error: {e}")


# ---------------------------------------------------------------------------
# Load profile seed / reset
# ---------------------------------------------------------------------------

@service
def battery_reset_load_profile(seed_w=None):
    """
    Initialise (or reset) both load-profile helpers to a flat profile.

    seed_w: watts to use for every bucket. Defaults to the current
            battery_avg_home_load_w value. Pass 0 to clear the profile.

    Call this once from Developer Tools → Services after first install, or
    whenever you want to wipe the learned curve and start fresh.
    """
    try:
        if seed_w is None:
            seed_w = _safe_float(
                _read_state("input_number.battery_avg_home_load_w", 400), 400
            )
        seed_w = max(0.0, min(9999.0, float(seed_w)))
        flat = [int(round(seed_w))] * 96
        am, pm = _format_load_profile(flat)
        _set_input_text("input_text.battery_load_profile_am", am)
        _set_input_text("input_text.battery_load_profile_pm", pm)
        _log_info(f"reset_load_profile: seeded all 96 buckets at {seed_w:.0f} W")
    except Exception as e:
        _log_info(f"reset_load_profile error: {e}")


# ---------------------------------------------------------------------------
# Cheapest price windows (for markdown dashboard card)
# ---------------------------------------------------------------------------

def _read_price_cache():
    """Concatenate the 5 price-cache chunks into the raw compact string."""
    parts = []
    for i in range(1, 6):
        chunk = _read_state(f"input_text.battery_tibber_prices_cache_{i}", "") or ""
        if chunk in ["unknown", "unavailable", "none", "None"]:
            continue
        parts.append(chunk)
    return "".join(parts)


def _best_window(values_ct, start_ts, start_idx, width_slots, horizon_slots):
    """
    Find the cheapest contiguous window of `width_slots` quarters within
    [start_idx, start_idx + horizon_slots) of `values_ct` (integer ct*10000).
    Returns dict {s,e,p} with ISO-strings and avg EUR/kWh, or empty placeholder.
    """
    n = len(values_ct)
    end = min(start_idx + horizon_slots, n)
    best_sum = None
    best_idx = -1
    last_start = end - width_slots
    if last_start < start_idx:
        return {"s": "", "e": "", "p": 0}

    # Sliding window sum
    window_sum = sum(values_ct[start_idx:start_idx + width_slots])
    best_sum = window_sum
    best_idx = start_idx
    for i in range(start_idx + 1, last_start + 1):
        window_sum += values_ct[i + width_slots - 1] - values_ct[i - 1]
        if window_sum < best_sum:
            best_sum = window_sum
            best_idx = i

    s_dt = start_ts + timedelta(minutes=15 * best_idx)
    e_dt = s_dt + timedelta(minutes=15 * width_slots)
    avg_eur = (best_sum / width_slots) / 10000.0
    return {
        "s": s_dt.strftime("%Y-%m-%dT%H:%M:%S"),
        "e": e_dt.strftime("%Y-%m-%dT%H:%M:%S"),
        "p": round(avg_eur, 5),
    }


@service
def battery_compute_price_windows():
    """
    For each N in [1,2,3,4] hours, find the cheapest contiguous window
    within the next 6h / 24h / 48h and write to
    input_text.tibber_window_{N}h as JSON:
      {"6h":{"s":ISO,"e":ISO,"p":EUR/kWh}, "24h":{...}, "48h":{...}}
    """
    try:
        import json

        raw = _read_price_cache()
        if ";" not in raw:
            return
        start_str, prices_str = raw.split(";", 1)
        if len(start_str) != 16:
            return
        try:
            start_ts = datetime.strptime(start_str, "%Y-%m-%dT%H:%M")
        except Exception:
            return

        values_ct = []
        for token in prices_str.split(","):
            token = token.strip()
            if not token:
                continue
            try:
                values_ct.append(int(token))
            except Exception:
                continue
        if not values_ct:
            return

        now_dt = datetime.now()
        start_idx = int((now_dt - start_ts).total_seconds() // 900)
        if start_idx < 0:
            start_idx = 0
        if start_idx >= len(values_ct):
            return

        horizons = {"6h": 24, "24h": 96, "48h": 192}
        sizes = [1, 2, 3, 4]
        empty = {"s": "", "e": "", "p": 0}

        for h in sizes:
            width = h * 4
            out = {}
            for hk, hslots in horizons.items():
                if hslots < width:
                    out[hk] = empty
                    continue
                out[hk] = _best_window(values_ct, start_ts, start_idx, width, hslots)
            _set_input_text(f"input_text.tibber_window_{h}h", json.dumps(out, separators=(",", ":")))

    except Exception as e:
        _log_info(f"compute_price_windows error: {e}")


