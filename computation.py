from pythermalcomfort.models import discomfort_index, heat_index, utci

UTCI_WIND_LOWER_BOUND = 0.50001
UTCI_WIND_UPPER_BOUND = 16.99999


def saturate(value: float, lower_bound: float, upper_bound: float) -> float:
    """
    Applies saturation to a value, keeping it between its lower and upper bounds.

    Args:
        value (float): Value to be saturated.
        lower_bound (float): Lower boundary.
        upper_bound (float): Upper boundary.

    Returns:
        float: Value after applying saturation.
    """
    return min(max(value, lower_bound), upper_bound)


def compute_heat_index(
        dry_bulb_temperature: list[float],
        relative_humidity: list[int]) -> list[float]:
    """
    Imports Heat Index function from 'pythermalcomfort.models' and uses it to compute the
    model values for an array of values of temperature and relative humidity. Inputs must
    have the same size.

    Args:
        dry_bulb_temperature (list[float]): Dry bulb temperature values.
        relative_humidity (list[int]): Relative humidity values.

    Returns:
        list[float]: Heat index calculated for each item in the input, with same size
        as inputs.
    """
    return [heat_index(tdb, rh) for tdb, rh in zip(
        dry_bulb_temperature, relative_humidity, strict=True)]


def compute_comfort_models(
        dry_bulb_temperature: list[float],
        relative_humidity: list[int],
        wind_speed: list[float],
        limit_utci_inputs=True,
        ) -> dict[str, list]:
    """
    Imports Discomfort Index and UTCI functions from 'pythermalcomfort.models', and returns
    a dictionary with lists of each output, including Heat Index.

    Args:
        dry_bulb_temperature (list[float]): Dry bulb temperature values
        relative_humidity (list[int]): Relative humidity values.
        wind_speed (list[float]): Wind speed values.
        limit_utci_inputs (bool): Same as 'limit_inputs' flag for 'utci' model. If false, a
            saturation function is used instead for Wind Speed (ensuring values are kept
            inside the model's working range).

    Returns:
        dict[str, list]: Object with one key for each output from the models, corresponding
        values are lists of the computed variables.
    """
    discomfort_model = discomfort_index(
        tdb=dry_bulb_temperature,
        rh=relative_humidity)

    if limit_utci_inputs:
        wind_speed = [
            saturate(v, UTCI_WIND_LOWER_BOUND, UTCI_WIND_UPPER_BOUND) for v in wind_speed]
    utci_model = utci(
        tdb=dry_bulb_temperature,
        tr=dry_bulb_temperature,
        v=wind_speed,
        rh=relative_humidity,
        return_stress_category=True,
        limit_inputs=limit_utci_inputs)
    return {
        "discomfort_index": discomfort_model.get("di"),
        "discomfort_condition": discomfort_model.get("discomfort_condition"),
        "heat_index": compute_heat_index(dry_bulb_temperature, relative_humidity),
        "utci": utci_model.get("utci"),                         # type: ignore
        "stress_category": utci_model.get("stress_category")    # type: ignore
    }
