from uuid import getnode as get_mac

from ttt.address import TTAddress


mV_BANDGAP = 1100


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


def compute_battery_voltage_rev_3_2(adc_volt_bat: int, adc_bandgap: int) -> float:
    return 2 * mV_BANDGAP * (float(adc_volt_bat) / float(adc_bandgap))


def compute_battery_voltage_rev_3_1(voltage: int) -> float:
    return 650 + (131072 * (1100 / voltage))


def generate_tt_address() -> TTAddress:
    return TTAddress(address=(get_mac() % 10000000000))
