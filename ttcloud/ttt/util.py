def compute_temperature(measurement: int) -> float:
    return round(
        (
            127.6
            - (0.006045 * measurement)
            + (1.26e-07 * (measurement ** 2))
            - (1.15e-12 * (measurement ** 3))
        ),
        2,
    )
