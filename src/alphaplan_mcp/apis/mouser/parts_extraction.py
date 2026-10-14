from __future__ import annotations

import math
import re
from typing import Any


def extract_purchasing_parts(response: Any) -> list[dict[str, Any]]:
    """
    Extract the Mouser parts of a part-number search.

    Always returns a list and does not raise for malformed API data.
    Parts without price breaks are kept (with `price_breaks: []`): Mouser lists such parts
    but will not quote them, typically with a `restriction` such as "not available for
    purchase by distributors". Dropping them made a listed part look like "not at Mouser".
    """
    try:
        if not isinstance(response, dict):
            return []

        search_results = response.get("SearchResults")
        if not isinstance(search_results, dict):
            return []

        raw_parts = search_results.get("Parts")
        if not isinstance(raw_parts, list):
            return []

        result: list[dict[str, Any]] = []

        for raw_part in raw_parts:
            if not isinstance(raw_part, dict):
                continue

            prices = _extract_prices(raw_part.get("PriceBreaks"))

            part = {
                "manufacturer": _string(raw_part.get("Manufacturer")),
                "manufacturer_part_number": _string(
                    raw_part.get("ManufacturerPartNumber")
                ),
                "mouser_part_number": _string(
                    raw_part.get("MouserPartNumber")
                ),
                "availability": {
                    "in_stock": _non_negative_int(
                        raw_part.get("AvailabilityInStock")
                    ),
                    "factory_stock": _non_negative_int(
                        raw_part.get("FactoryStock")
                    ),
                    "on_order": _safe_list(
                        raw_part.get("AvailabilityOnOrder")
                    ),
                    "lead_time": _string(raw_part.get("LeadTime")),
                },
                "ordering": {
                    "minimum_quantity": _non_negative_int(
                        raw_part.get("Min")
                    ),
                    "order_multiple": _non_negative_int(
                        raw_part.get("Mult")
                    ),
                    "reeling": _bool_or_none(raw_part.get("Reeling")),
                },
                "price_breaks": prices,
                "lifecycle_status": _string(
                    raw_part.get("LifecycleStatus")
                ),
                "restriction": _string(
                    raw_part.get("RestrictionMessage")
                ),
                "suggested_replacement": _string(
                    raw_part.get("SuggestedReplacement")
                ),
                "product_url": _string(
                    raw_part.get("ProductDetailUrl")
                ),
            }

            result.append(_remove_none_values(part))

        return result

    except Exception:
        # MCP tools should return a predictable result rather than leak
        # parsing failures to the caller.
        return []


def _extract_prices(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []

    prices: list[dict[str, Any]] = []

    for item in value:
        try:
            if not isinstance(item, dict):
                continue

            quantity = _positive_int(item.get("Quantity"))
            unit_price = _parse_price(item.get("Price"))
            currency = _string(item.get("Currency"))

            # A purchasing price is unusable without these fields.
            if quantity is None or unit_price is None or not currency:
                continue

            prices.append(
                {
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "currency": currency.upper(),
                }
            )
        except Exception:
            continue

    prices.sort(key=lambda price: price["quantity"])
    return prices


def _parse_price(value: Any) -> float | None:
    if isinstance(value, bool):
        return None

    if isinstance(value, (int, float)):
        number = float(value)
        return number if math.isfinite(number) and number >= 0 else None

    if not isinstance(value, str):
        return None

    # Keep digits, separators, and sign; discard currency symbols/text.
    cleaned = re.sub(r"[^\d,.\-]", "", value.strip())
    if not cleaned:
        return None

    if "," in cleaned and "." in cleaned:
        # The rightmost separator is assumed to be the decimal separator.
        if cleaned.rfind(",") > cleaned.rfind("."):
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    elif "," in cleaned:
        cleaned = cleaned.replace(",", ".")

    try:
        number = float(cleaned)
    except (TypeError, ValueError, OverflowError):
        return None

    return number if math.isfinite(number) and number >= 0 else None


def _string(value: Any) -> str | None:
    if not isinstance(value, str):
        return None

    value = value.strip()
    return value or None


def _positive_int(value: Any) -> int | None:
    number = _int(value)
    return number if number is not None and number > 0 else None


def _non_negative_int(value: Any) -> int | None:
    number = _int(value)
    return number if number is not None and number >= 0 else None


def _int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None

    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return None

    return number


def _bool_or_none(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    return None


def _safe_list(value: Any) -> list[Any]:
    if not isinstance(value, list):
        return []

    # Retain only values that are already JSON-safe.
    return [
        item
        for item in value
        if item is None or isinstance(item, (str, int, float, bool, dict, list))
    ]


def _remove_none_values(value: Any) -> Any:
    """Recursively omit missing fields while retaining False, zero and []."""
    if isinstance(value, dict):
        return {
            key: _remove_none_values(item)
            for key, item in value.items()
            if item is not None
        }

    if isinstance(value, list):
        return [_remove_none_values(item) for item in value]

    return value
