# This script runs a batch job that calculates a rolling average for stock price data.
# How to use:
# python run.py --input "data.csv" --config "config.yaml" --output "metrics.json" --log-file "run.log"

import argparse
import json
import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

# --- Set up the logging system ---

def prepare_logger(log_file_path: str) -> logging.Logger:
    # We want to log things to both a file and the screen.
    messenger = logging.getLogger("task_runner")
    messenger.setLevel(logging.DEBUG)

    # Define how the log messages should look.
    line_format = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Save logs to a file.
    file_handler = logging.FileHandler(log_file_path, mode="w")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(line_format)
    messenger.addHandler(file_handler)

    # Also show logs on the screen.
    screen_handler = logging.StreamHandler(sys.stderr)
    screen_handler.setLevel(logging.INFO)
    screen_handler.setFormatter(line_format)
    messenger.addHandler(screen_handler)

    return messenger

def save_results(output_path: str, results_summary: dict) -> None:
    # Save the results into a JSON file.
    with open(output_path, "w") as file_writer:
        json.dump(results_summary, file_writer, indent=2)
    
    # Also print the results so we can see them immediately.
    print(json.dumps(results_summary, indent=2))

def build_error_report(script_version: str, error_message: str) -> dict:
    # Create a simple dictionary if something goes wrong.
    return {
        "version": script_version,
        "status": "error",
        "error_message": error_message,
    }


# --- Validation and Loading ---

REQUIRED_SETTING_KEYS = {"seed", "window", "version"}

def read_settings(config_file_path: str, messenger: logging.Logger) -> dict:
    # Load the YAML configuration and make sure it has everything we need.
    path_object = Path(config_file_path)
    if not path_object.exists():
        raise FileNotFoundError(f"Sorry, I couldn't find the config file at: {config_file_path}")

    with open(path_object, "r") as file_reader:
        settings = yaml.safe_load(file_reader)

    if not isinstance(settings, dict):
        raise ValueError("The config file needs to be a list of settings (key-value pairs).")

    missing_keys = REQUIRED_SETTING_KEYS - settings.keys()
    if missing_keys:
        raise ValueError(f"The config file is missing these required settings: {missing_keys}")

    # Checking data types to be safe.
    if not isinstance(settings["seed"], int):
        raise ValueError(f"The 'seed' should be a number, but I got {type(settings['seed']).__name__}.")
    if not isinstance(settings["window"], int) or settings["window"] < 1:
        raise ValueError(f"The 'window' should be a positive number, but I got {settings['window']}.")
    if not isinstance(settings["version"], str):
        raise ValueError(f"The 'version' should be text, but I got {type(settings['version']).__name__}.")

    messenger.info("Settings loaded and checked!")
    messenger.info(f"  Seed value: {settings['seed']}")
    messenger.info(f"  Calculation window: {settings['window']}")
    messenger.info(f"  Version: {settings['version']}")

    return settings

def read_data_file(data_file_path: str, messenger: logging.Logger) -> pd.DataFrame:
    # Load the CSV data and make sure it has the 'close' price column.
    path_object = Path(data_file_path)
    if not path_object.exists():
        raise FileNotFoundError(f"Sorry, I couldn't find the data file at: {data_file_path}")

    try:
        # We specify the separator as a comma explicitly to meet the task requirements.
        data_table = pd.read_csv(path_object, sep=",")
    except Exception as error:
        raise ValueError(f"There's something wrong with the CSV format: {error}")

    if data_table.empty:
        raise ValueError("The data file appears to be empty.")

    if "close" not in data_table.columns:
        raise ValueError(f"The data file is missing the 'close' column. It only has: {list(data_table.columns)}")

    # Make sure the prices are actually numbers.
    if not np.issubdtype(data_table["close"].dtype, np.number):
        raise ValueError("The 'close' column has values that aren't numbers.")

    messenger.info(f"Data loaded successfully: {len(data_table)} rows found.")
    return data_table


# --- Core Logic for Processing ---

def calculate_average_trend(data_table: pd.DataFrame, window_size: int, messenger: logging.Logger) -> pd.Series:
    # Calculate the moving average of the 'close' prices.
    moving_average = data_table["close"].rolling(window=window_size).mean()
    empty_rows = moving_average.isna().sum()
    messenger.info(f"Calculated the average trend over {window_size} days. Skipped {empty_rows} rows at the start.")
    return moving_average

def decide_on_signal(prices: pd.Series, averages: pd.Series, messenger: logging.Logger) -> pd.Series:
    # We create a simple signal: 1 if the price is above the average, and 0 otherwise.
    signal_results = (prices > averages).astype(float)
    
    # If the average is missing (NaN), the signal should also be missing.
    signal_results[averages.isna()] = np.nan
    
    clean_signals = signal_results.dropna()
    messenger.info(
        f"Signal generation complete! {len(clean_signals)} valid points calculated. "
        f"Found {int(clean_signals.sum())} 'up' signals and {int((clean_signals == 0).sum())} 'down' signals."
    )
    return signal_results


# --- Program Entry and Main Loop ---

def get_user_inputs() -> argparse.Namespace:
    # Set up the command-line arguments.
    parser = argparse.ArgumentParser(description="A tool that checks stock trends and saves metrics.")
    parser.add_argument("--input", required=True, help="Path to your data file (CSV)")
    parser.add_argument("--config", required=True, help="Path to your settings file (YAML)")
    parser.add_argument("--output", required=True, help="Where to save the results (JSON)")
    parser.add_argument("--log-file", required=True, help="Where to save the logs (txt)")
    return parser.parse_args()

def run_pipeline() -> int:
    inputs = get_user_inputs()
    messenger = prepare_logger(inputs.log_file)

    script_version = "unknown"
    start_timestamp = time.time()

    try:
        messenger.info("============================================================")
        messenger.info("STARTING THE JOB")
        messenger.info("============================================================")

        # 1. Load settings
        settings = read_settings(inputs.config, messenger)
        script_version = settings["version"]
        luck_factor = settings["seed"]
        window_size = settings["window"]

        # Set the random seed for consistency.
        np.random.seed(luck_factor)
        messenger.info(f"Set the random seed to {luck_factor}")

        # 2. Load data
        data_table = read_data_file(inputs.input, messenger)

        # 3. Calculate trend
        messenger.info("Working on the rolling average calculations...")
        average_trend = calculate_average_trend(data_table, window_size, messenger)

        # 4. Generate trend signals
        messenger.info("Generating signals based on price vs average...")
        trend_signals = decide_on_signal(data_table["close"], average_trend, messenger)

        # 5. Compile and save results
        valid_points = trend_signals.dropna()
        how_many_rows = len(valid_points)
        average_signal_value = round(float(valid_points.mean()), 4)
        time_taken_ms = round((time.time() - start_timestamp) * 1000)

        final_metrics = {
            "version": script_version,
            "rows_processed": how_many_rows,
            "metric": "signal_rate",
            "value": average_signal_value,
            "latency_ms": time_taken_ms,
            "seed": luck_factor,
            "status": "success",
        }

        messenger.info("Here is a summary of the results:")
        for key, value in final_metrics.items():
            messenger.info(f"  {key}: {value}")

        save_results(inputs.output, final_metrics)

        messenger.info("============================================================")
        messenger.info("JOB FINISHED SUCCESSFULLY")
        messenger.info("============================================================")
        return 0

    except Exception as error_occurred:
        time_taken_ms = round((time.time() - start_timestamp) * 1000)
        messenger.exception(f"Something went wrong: {error_occurred}")

        error_report = build_error_report(script_version, str(error_occurred))
        error_report["latency_ms"] = time_taken_ms
        save_results(inputs.output, error_report)

        messenger.info("============================================================")
        messenger.info("JOB ENDED WITH ERRORS")
        messenger.info("============================================================")
        return 1

if __name__ == "__main__":
    sys.exit(run_pipeline())
 
