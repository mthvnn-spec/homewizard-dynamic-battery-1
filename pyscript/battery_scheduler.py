from datetime import datetime, timedelta

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


def _slot_hour(slot_time):
    return int(slot_time[11:13])


def _slot_date(slot_time):
    return slot_time[:10]


def _to_u(kwh, soc_unit):
    return int(round(float(kwh) / float(soc_unit)))


def _is_overnight_before_morning(slot_time, today_str, tomorrow_str, morning_peak_end_hour):
    hh = _slot_hour(slot_time)
    d = _slot_date(slot_time)
    return (
        (d == today_str and hh >= 18)
        or (d == tomorrow_str and hh < morning_peak_end_hour)
    )


def _percentile(sorted_values, fraction):
    if not sorted_values:
        return 0.0
    idx = max(0, min(len(sorted_values) - 1, int(len(sorted_values) * fraction)))
    return sorted_values[idx]


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
    max_discharge_slot_kwh,
    discharge_delivery_kwh,
    discharge_efficiency_from_battery,
    charge_cap_by_slot,
    required_margin,
    min_soc_floor_kwh,
):
    """
    DP over future 15-min slots.
    - charge cost uses actual grid kWh drawn (max_charge_slot_kwh / charge_efficiency)
    - discharge revenue uses delivered kWh after discharge efficiency
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
    discharge_u = max(_to_u(max_discharge_slot_kwh, SOC_UNIT), 1)
    init_u = max(0, min(capacity_u, _to_u(initial_soc_kwh, SOC_UNIT)))
    floor_u = _to_u(min_soc_floor_kwh, SOC_UNIT)

    NSTATES = capacity_u + 1

    terminal_value_per_u = SOC_UNIT * float(discharge_efficiency_from_battery) * terminal_price
    V = [max(0, s - floor_u) * terminal_value_per_u for s in range(NSTATES)]
    policy_stack = []

    _log_info(
        f"dp N={N} cap_u={capacity_u} states={NSTATES} "
        f"terminal_price={terminal_price:.4f} "
        f"terminal_val_per_u={terminal_value_per_u:.5f} "
        f"delivery_kwh={discharge_delivery_kwh:.4f}"
    )

    for t in range(N - 1, -1, -1):
        price = float(prices[t]["price"])
        c_cost = (max_charge_slot_kwh / charge_efficiency_to_battery) * price
        d_rev = discharge_delivery_kwh * price - (discharge_delivery_kwh * required_margin)
        cap_u = min(_to_u(charge_cap_by_slot[t], SOC_UNIT), capacity_u)

        new_V = [0.0] * NSTATES
        new_pol = [0] * NSTATES

        for s in range(NSTATES):
            best = V[s]
            best_a = 0

            if s < cap_u:
                sc = min(s + charge_gain_u, cap_u)
                vc = V[sc] - c_cost
                if vc > best:
                    best, best_a = vc, 1

            if s - discharge_u >= floor_u:
                sd = s - discharge_u
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
            cap_u = min(_to_u(charge_cap_by_slot[t], SOC_UNIT), capacity_u)
            s = min(s + charge_gain_u, cap_u)
        elif act == 2:
            s = max(s - discharge_u, 0)

    actual_value = 0.0
    for t in range(N):
        if actions[t] == 1:
            actual_value -= (max_charge_slot_kwh / charge_efficiency_to_battery) * float(prices[t]["price"])
        elif actions[t] == 2:
            actual_value += discharge_delivery_kwh * float(prices[t]["price"])

    return actions, actual_value


# ---------------------------------------------------------------------------
# Schedule builder
# ---------------------------------------------------------------------------

def _build_schedule(
    prices,
    soc,
    capacity_kwh,
    max_charge_slot_kwh,
    max_discharge_slot_kwh,
    charge_efficiency_to_battery,
    discharge_efficiency_from_battery,
    min_profit,
    safety_margin,
    min_total_value,
    min_price_spread,
    today_str,
    tomorrow_str,
    morning_reserve_fraction,
    morning_reserve_min_slots,
    morning_peak_end_hour,
    solar_forecast_tomorrow_kwh,
    min_discharge_soc,
):
    required_margin = float(min_profit) + float(safety_margin)
    start_energy_kwh = max(
        0.0, min(float(capacity_kwh), (float(soc) / 100.0) * float(capacity_kwh))
    )
    charge_gain_kwh = float(max_charge_slot_kwh) * float(charge_efficiency_to_battery)
    discharge_delivery_kwh = float(max_discharge_slot_kwh) * float(discharge_efficiency_from_battery)
    min_soc_floor_kwh = float(capacity_kwh) * (float(min_discharge_soc) / 100.0)

    saldering_enabled = _read_bool("input_boolean.battery_saldering_enabled", True)
    tomorrow_negative_price = any([
        slot["start_time"][:10] == tomorrow_str and float(slot["price"]) < 0
        for slot in prices
    ])

    # Solar headroom only active when saldering is OFF (post-2027) or when
    # tomorrow has negative prices. While saldering is on, exported solar
    # earns full import price so keeping the battery empty costs arbitrage profit.
    solar_headroom_active = (
        float(solar_forecast_tomorrow_kwh) > 0.8
        and (not saldering_enabled or tomorrow_negative_price)
    )

    if solar_headroom_active:
        # Scale relative to battery capacity
        solar_scale = min(float(solar_forecast_tomorrow_kwh) / float(capacity_kwh), 1.0)
        effective_reserve_fraction = float(morning_reserve_fraction) * solar_scale
        charge_cap_soc = max(10.0, (1.0 - effective_reserve_fraction) * 100.0)
    else:
        charge_cap_soc = 100.0

    charge_cap_kwh = float(capacity_kwh) * (charge_cap_soc / 100.0)
    min_morning_reserve_kwh = float(max_discharge_slot_kwh) * int(morning_reserve_min_slots)
    overnight_cap_kwh = max(charge_cap_kwh, min_morning_reserve_kwh)

    if not prices:
        _log_info("no prices available after parsing cache")
        return {
            "charge_slots": [], "discharge_slots": [],
            "expected_profit": 0.0, "total_expected_value": 0.0,
            "charge_ceiling": 0.0, "discharge_floor": 0.0,
            "price_spread": 0.0, "profitable": False,
            "charge_cap_soc": 0.0, "charge_cap_kwh": 0.0,
            "solar_headroom_active": False,
        }

    charge_cap_by_slot = []
    for slot in prices:
        if solar_headroom_active and _is_overnight_before_morning(
            slot["start_time"], today_str, tomorrow_str, int(morning_peak_end_hour)
        ):
            charge_cap_by_slot.append(overnight_cap_kwh)
        else:
            charge_cap_by_slot.append(float(capacity_kwh))

    actions, actual_value = _dp_optimize(
        prices=prices,
        initial_soc_kwh=start_energy_kwh,
        capacity_kwh=float(capacity_kwh),
        max_charge_slot_kwh=float(max_charge_slot_kwh),
        charge_gain_kwh=charge_gain_kwh,
        charge_efficiency_to_battery=float(charge_efficiency_to_battery),
        max_discharge_slot_kwh=float(max_discharge_slot_kwh),
        discharge_delivery_kwh=discharge_delivery_kwh,
        discharge_efficiency_from_battery=float(discharge_efficiency_from_battery),
        charge_cap_by_slot=charge_cap_by_slot,
        required_margin=required_margin,
        min_soc_floor_kwh=min_soc_floor_kwh,
    )

    charge_slots = []
    discharge_slots = []
    for i, act in enumerate(actions):
        if act == 1:
            charge_slots.append({"start_time": prices[i]["start_time"], "price": prices[i]["price"]})
        elif act == 2:
            discharge_slots.append({"start_time": prices[i]["start_time"], "price": prices[i]["price"]})

    n_discharge = len(discharge_slots)
    total_expected_value = actual_value

    expected_profit = (
        total_expected_value / (n_discharge * discharge_delivery_kwh)
        if n_discharge > 0 and discharge_delivery_kwh > 0 else 0.0
    )

    charge_ceiling = max([s["price"] for s in charge_slots]) if charge_slots else 0.0
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
        f"solar_headroom={solar_headroom_active} charge_cap_soc={charge_cap_soc:.1f} "
        f"discharge_delivery_kwh={discharge_delivery_kwh:.4f}"
    )

    return {
        "charge_slots": charge_slots,
        "discharge_slots": discharge_slots,
        "expected_profit": round(expected_profit, 4),
        "total_expected_value": round(total_expected_value, 4),
        "charge_ceiling": round(charge_ceiling, 4),
        "discharge_floor": round(discharge_floor, 4),
        "price_spread": round(price_spread, 4),
        "profitable": profitable,
        "charge_cap_soc": round(charge_cap_soc, 1),
        "charge_cap_kwh": round(charge_cap_kwh, 4),
        "solar_headroom_active": solar_headroom_active,
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
        tomorrow_str = (now_dt + timedelta(days=1)).strftime("%Y-%m-%d")
        current_slot = _current_slot_iso(now_dt)
        _set_input_text("input_text.battery_debug_stage", f"time ok {current_slot}")

        soc = _safe_float(_read_state("sensor.plug_in_battery_state_of_charge", 50), 50)
        soc = min(100.0, max(0.0, soc))
        _set_input_text("input_text.battery_debug_stage", f"soc ok {soc}")

        min_profit = _safe_float(_read_state("input_number.battery_min_profit_threshold", 0.05), 0.05)
        safety_margin = _safe_float(_read_state("input_number.battery_schedule_safety_margin", 0.02), 0.02)
        min_total_value = _safe_float(_read_state("input_number.battery_schedule_min_total_value", 0.03), 0.03)
        min_discharge_soc = _safe_float(_read_state("input_number.battery_min_discharge_soc", 5), 5)

        capacity_kwh = _safe_float(_read_state("input_number.battery_capacity_kwh", 2.7), 2.7)
        max_charge_slot_kwh = _safe_float(_read_state("input_number.battery_max_charge_slot_kwh", 0.189), 0.189)
        max_discharge_slot_kwh = _safe_float(_read_state("input_number.battery_max_discharge_slot_kwh", 0.189), 0.189)

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

        morning_reserve_fraction = _safe_float(_read_state("input_number.battery_morning_reserve_fraction", 0.35), 0.35)
        morning_reserve_min_slots = int(_safe_float(_read_state("input_number.battery_morning_reserve_min_slots", 2), 2))
        morning_peak_end_hour = int(_safe_float(_read_state("input_number.battery_morning_peak_end_hour", 10), 10))

        solar_forecast_tomorrow_kwh = _safe_float(
            _read_state("input_number.battery_solcast_tomorrow_cached", 0), 0
        )
        if solar_forecast_tomorrow_kwh < 0.1:
            solar_forecast_tomorrow_kwh = _safe_float(
                _read_state("sensor.solcast_pv_forecast_forecast_tomorrow", 0), 0
            )

        solar_accuracy_ratio = _safe_float(
            _read_state("input_number.battery_solar_forecast_accuracy", 1.0), 1.0
        )
        if solar_accuracy_ratio != 1.0:
            raw_forecast = solar_forecast_tomorrow_kwh
            solar_forecast_tomorrow_kwh = raw_forecast * solar_accuracy_ratio
            _log_info(
                f"solar forecast adjusted {raw_forecast:.2f} * {solar_accuracy_ratio:.2f} "
                f"= {solar_forecast_tomorrow_kwh:.2f} kWh"
            )
        _set_input_text("input_text.battery_debug_stage", f"solar ok {solar_forecast_tomorrow_kwh:.2f}")

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

        result = _build_schedule(
            prices=prices,
            soc=soc,
            capacity_kwh=capacity_kwh,
            max_charge_slot_kwh=max_charge_slot_kwh,
            max_discharge_slot_kwh=max_discharge_slot_kwh,
            charge_efficiency_to_battery=charge_efficiency_to_battery,
            discharge_efficiency_from_battery=discharge_efficiency_from_battery,
            min_profit=min_profit,
            safety_margin=safety_margin,
            min_total_value=min_total_value,
            min_price_spread=min_price_spread,
            today_str=today_str,
            tomorrow_str=tomorrow_str,
            morning_reserve_fraction=morning_reserve_fraction,
            morning_reserve_min_slots=morning_reserve_min_slots,
            morning_peak_end_hour=morning_peak_end_hour,
            solar_forecast_tomorrow_kwh=solar_forecast_tomorrow_kwh,
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
        enough_energy_now = ((soc / 100.0) * capacity_kwh) >= (max_discharge_slot_kwh * 0.5)

        soc_below_cap = soc < result["charge_cap_soc"]
        is_opportunistic = (
            opportunistic_threshold > 0
            and soc < 100
            and soc_below_cap
            and live_price >= 0
            and live_price <= opportunistic_threshold
            and profitable
            and (
                result["charge_ceiling"] == 0
                or live_price <= result["charge_ceiling"]
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
            target_mode = "zero"
        else:
            target_mode = "zero_charge_only"

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

        if result["solar_headroom_active"]:
            reason = f"{reason} | solar headroom cap {result['charge_cap_soc']:.0f}%"

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
            f"discharge_count={len(discharge_times)} "
            f"charge_cap_soc={result['charge_cap_soc']:.1f}"
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
                        f"Charge cap: {round(result['charge_cap_soc'], 0)}%\n"
                        f"{'📅 Cross-day' if has_tomorrow else '📅 Today only'}"
                    ),
                )
            elif target_mode == "zero":
                script.battery_notify(
                    title="⚡ Battery → Discharging",
                    message=(
                        f"Price: {round(live_price * 100, 1)} ct\n"
                        f"SOC: {int(soc)}%\n"
                        f"Slots: {len(charge_times)} charge / {len(discharge_times)} discharge\n"
                        f"Expected margin: {round(result['expected_profit'] * 100, 2)} ct/kWh\n"
                        f"Expected schedule value: €{round(result['total_expected_value'], 3)}\n"
                        f"Charge cap: {round(result['charge_cap_soc'], 0)}%\n"
                        f"{'📅 Cross-day' if has_tomorrow else '📅 Today only'}"
                    ),
                )
            elif last_mode in ["to_full", "zero"]:
                script.battery_notify(
                    title="🔄 Battery → Hold",
                    message=(
                        "No profitable schedule found\n" if not profitable
                        else "Waiting for scheduled slot\n"
                    ) + (
                        f"SOC: {int(soc)}% · Slots: {len(charge_times)} charge / {len(discharge_times)} discharge\n"
                        f"Charge cap: {round(result['charge_cap_soc'], 0)}%"
                    ),
                )

        _set_input_text("input_text.battery_debug_stage", "done")

    except Exception as e:
        _set_input_text("input_text.battery_debug_error", str(e))
        _set_input_text("input_text.battery_debug_stage", "crashed")
        raise
