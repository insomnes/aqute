def check_value_in_interval(
    value: float, lower: float, upper: float = 1_000_000.0, error: float = 0.001
) -> bool:
    more_than_lower = value >= lower
    if not more_than_lower:
        more_than_lower = abs(value - lower) <= error

    less_than_upper = value <= upper
    if not less_than_upper:
        less_than_upper = abs(value - upper) <= error

    return more_than_lower and less_than_upper
