#!/bin/python3

# This file will be scheduled by cron to run periodically

from datetime import datetime

import pandas as pd
import requests
import time

from config import config

# you can use OFFLINE_MODE during development to save API calls (uses local files instead):
OFFLINE_MODE = True

## QUERY DATA
# Query for flights with at least this much delays
# should be >30
MIN_DELAY = 31

# Delay type. "departures" or "arrivals"
DELAY_TYPE="departures"

# How many times we should retry downloading
# if our response is not 200 (OK)
ERROR_RETRIES = 5
ERROR_RETRY_WAIT_TIME = 1    # secs


def get_delays():
    if OFFLINE_MODE:
        delays = pd.read_csv("data/delays/delays-sample.csv")
        return delays
    else:
        retries = 0
        while retries < ERROR_RETRIES:
            url = (f"https://airlabs.co/api/v9/delays?delay={MIN_DELAY}"
                   f"&type={DELAY_TYPE}&api_key={config.api_key}")
            print(f"Requesting {url}")
            response = requests.get(url)
            data = response.json()
            if 'response' in data:
                delays = pd.json_normalize(data['response'])
                return delays

            retries += 1
            print("Request failed")
            time.sleep(ERROR_RETRY_WAIT_TIME)
    return None


def add_country_codes(delays, airports):
    airports["arr_iata"] = airports["iata_code"]
    airports["dep_iata"] = airports["iata_code"]
    airports["arr_country_code"] = airports["country_code"]
    airports["dep_country_code"] = airports["country_code"]
    delays = pd.merge(delays, airports[["arr_country_code", "arr_iata"]], on="arr_iata", how="inner")
    delays = pd.merge(delays, airports[["dep_country_code", "dep_iata"]], on="dep_iata", how="inner")
    return delays


def main():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', 5)

    delays = get_delays()

    if delays is None:
        print(f"ERROR! CAN'T FETCH DATA FROM AIRLAB!")
        return

    print(f"Total delays worldwide: {len(delays)}")
    airports = pd.read_json("data/airports.json")
    delays = add_country_codes(delays, airports)
    delays["domestic"] = delays["arr_country_code"] == delays["dep_country_code"]
    delays["international"] = ~delays["domestic"]
    print("Delays:")
    delays.info()
    print(delays)
    delays = delays[["flight_number", "airline_iata", "dep_time_utc", "dep_estimated_utc", "arr_time_utc",
                     "arr_estimated_utc", "dep_country_code", "arr_country_code", "domestic", "international", "delayed"]]

    # Appends to existing file
    delays.to_csv(f"data/delays/delays-{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv", mode='a')


if __name__ == '__main__':
    main()
