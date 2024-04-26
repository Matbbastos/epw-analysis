#! /usr/bin/env python3.11
import logging
import statistics
import time
from argparse import ArgumentParser
from datetime import datetime
from pathlib import Path

import polars as pl
from ladybug.epw import EPW
from polars.datatypes import Datetime, Float64, Int32, Int64, String


def validate_io_paths(args) -> dict[str, Path]:
    """
    Validate path to input dir and chosen output file path and return corresponding
    Path objects.

    Args:
        args (argparse.Namespace): Namespace containing the arguments parsed from
            command line.

    Raises:
        ValueError: If input path is not a directory.

    Returns:
        dict[str, Path]: Input directory and output file Path objects.
    """
    input_dir = Path(args.path)
    if not input_dir.is_dir():
        raise ValueError(
            f"The provided path is not a directory. Value: {input_dir.resolve()}")

    if args.output is None:
        output_filename = Path(
            datetime.now().strftime("%Y-%m-%d %H_%M") + " compiled_to"
            ).with_suffix(".parquet")
        logging.info(
            f"Output file not provided, using default with current timestamp. "
            f"Output path: {output_filename.resolve()}")
    else:
        output_filename = Path(args.output).with_suffix(".parquet")

    return {
        "input_dir": input_dir,
        "output_filename": output_filename
        }


def list_epw_files(directory: Path) -> list[Path]:
    """
    Returns a list of all EPW files found in the directory.

    Args:
        directory (Path): Directory where to look for EPW files.

    Raises:
        ValueError: If there are no EPW files in the directory.

    Returns:
        list[Path]: List of Path objects for each EPW file found in the directory.
    """
    epw_file_collection = [
        file for file in directory.iterdir() if file.suffix.casefold() == ".epw".casefold()]
    if not epw_file_collection:
        logging.warning("No EPW files found in the selected path")
        raise ValueError("Selected path contains no EPW files.")
    logging.info(
        f"Found {len(epw_file_collection)} EPW file(s) in the selected path "
        f"({directory.resolve()})")
    return epw_file_collection


def parse_filename(file_path: Path) -> dict[str, str | int]:
    """
    Parses a file name to provide values for scenario and year.

    Parses the file name looking for information on scenario and year. Requires scenarios to
    be identified by the 'ssp' code and file name to resemble the end of one of the
    following patterns:
        some_city_WHATEVER_2021.epw
        some_city_WHATEVER_ssp126_2050.epw
    That is:
        - year must be the last 4 characters before extension
        - if scenario is present, it must be right before year, separated by an underscore

    Args:
        file_path (Path): Path object that points to the file to be parsed.

    Returns:
        dict[str, str | int]: Scenario and Year key and values.
    """
    scenario = "Baseline"
    year = int(file_path.stem[-4:])
    if "ssp".casefold() in file_path.stem.casefold():
        scenario = file_path.stem.split("_")[-2].upper()

    return {
        "scenario": scenario,
        "year": year,
    }


def compute_heat_index(
        dry_bulb_temperature: tuple[float],
        relative_humidity: tuple[int]) -> list[float]:
    """
    Imports Heat Index function from 'pythermalcomfort.models' and uses it to compute the
    model values for an array of values of temperature and relative humidity. Inputs must
    have the same size.

    Args:
        dry_bulb_temperature (tuple[float]): Dry bulb temperature values.
        relative_humidity (tuple[int]): Relative humidity values.

    Returns:
        list[float]: Heat index calculated for each item in the input, with same size
        as inputs.
    """
    from pythermalcomfort.models import heat_index
    return [heat_index(tdb, rh) for tdb, rh in zip(
        dry_bulb_temperature, relative_humidity, strict=True)]


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


def main(args):
    setup_start = time.perf_counter()
    logging.basicConfig(
        format="%(asctime)s    %(levelname)-8.8s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )
    if args.quiet:
        logging.getLogger().setLevel(logging.WARNING)

    input_dir, output_filename = validate_io_paths(args).values()

    epw_file_collection = list_epw_files(input_dir)

    output_df = pl.DataFrame(
        schema={
            "City": String,
            "State": String,
            "Latitude": Float64,
            "Longitude": Float64,
            "Elevation": Float64,
            "Scenario/Code": String,
            "Scenario/Year": Int32,
            "Datetime": Datetime,
            "Dry Bulb Temperature": Float64,
            "Dew Point Temperature": Float64,
            "Relative Humidity": Int64,
            "Atmospheric Station Pressure": Int64,
            "Horizontal Infrared Radiation Intensity": Int64,
            "Direct Normal Radiation": Int64,
            "Diffuse Horizontal Radiation": Int64,
            "Wind Direction": Int64,
            "Wind Speed": Float64,
            "Total Sky Cover": Int64,
            "Opaque Sky Cover": Int64},
        strict=False)

    process_metrics = {
        "duration": [],
        "exception_counter": 0,
        "exported_csv": False
    }

    logging.info(f"Setup time: {time.perf_counter() - setup_start:.3f}s")
    for index, file in enumerate(sorted(epw_file_collection)):
        start_time = time.perf_counter()
        logging.info(f"Processing started for file '{file.name}'")

        epw_file = EPW(file)
        scenario, year = parse_filename(file).values()
        logging.info(f"Found scenario='{scenario}' and year={year} for current file")
        try:
            current_data = pl.DataFrame({
                "City": epw_file.location.city.replace(".", " "),
                "State": epw_file.location.state,
                "Latitude": epw_file.location.latitude,
                "Longitude": epw_file.location.longitude,
                "Elevation": epw_file.location.elevation,
                "Scenario/Code": scenario,
                "Scenario/Year": year,
                "Datetime": epw_file.dry_bulb_temperature.datetimes,
                "Dry Bulb Temperature": epw_file.dry_bulb_temperature.values,
                "Dew Point Temperature": epw_file.dew_point_temperature.values,
                "Relative Humidity": epw_file.relative_humidity.values,
                "Atmospheric Station Pressure":
                    epw_file.atmospheric_station_pressure.values,
                "Horizontal Infrared Radiation Intensity":
                    epw_file.horizontal_infrared_radiation_intensity.values,
                "Direct Normal Radiation": epw_file.direct_normal_radiation.values,
                "Diffuse Horizontal Radiation":
                    epw_file.diffuse_horizontal_radiation.values,
                "Wind Direction": epw_file.wind_direction.values,
                "Wind Speed": epw_file.wind_speed.values,
                "Total Sky Cover": epw_file.total_sky_cover.values,
                "Opaque Sky Cover": epw_file.opaque_sky_cover.values},
                )
        except Exception as e:
            logging.exception(
                f"Could not extend the output dataframe with data from current file "
                f"({file.name}). Exception: {str(e)}")
            process_metrics["exception_counter"] += 1
        else:
            output_df.extend(current_data)
            file_counter = index + 1 - process_metrics["exception_counter"]
            logging.info(
                f"({file_counter}/{len(epw_file_collection)}) Added data from "
                f"file '{file.name}' to the output dataframe")
        process_metrics["duration"].append(time.perf_counter() - start_time)

        if args.export_csv and not process_metrics["exported_csv"]:
            current_data.write_csv(Path(output_filename.stem).with_suffix(".csv"))
            process_metrics["exported_csv"] = True
            logging.info(f"Generated sample CSV file containing data from '{file.name}'")

    if output_df.is_empty():
        logging.warning(
            "No data was gathered from EPW files, no output file will be generated")
        return

    process_metrics.update({
        "merged_files_count": len(process_metrics["duration"]),
        "max_time_ms": round(1000*max(process_metrics["duration"])),
        "mean_time_ms": round(1000*statistics.mean(process_metrics["duration"])),
        "min_time_ms": round(1000*min(process_metrics["duration"])),
    })
    logging.info(
        f"Processed a total of {process_metrics['merged_files_count']} files "
        f"(Max time: {process_metrics['max_time_ms']}ms, "
        f"Mean time: {process_metrics['mean_time_ms']}ms, "
        f"Min time: {process_metrics['min_time_ms']}ms)")

    output_df.write_parquet(output_filename)
    logging.info(f"Generated output .parquet file at '{output_filename.resolve()}'")


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument(
        "path", help="path of the directory containing the EPW files.", type=str)
    parser.add_argument(
        "-o", "--output", metavar="path/out.parquet",
        help="output filename. Defaults to a standard name with timestamp.", type=str)
    parser.add_argument(
        "-q", "--quiet", action="store_true",
        help="turn on quiet mode, which hides log entries of levels lower than WARNING.")
    parser.add_argument(
        "--csv", action="store_true", dest="export_csv",
        help="export an extra sample CSV output file from the first EPW file that is "
        "processed, for output validation.")
    main(parser.parse_args())
