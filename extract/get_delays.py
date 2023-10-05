from datetime import datetime

import pandas as pd
import requests

from extract.config import config


# you can use OFFLINE_MODE during development to save API calls (uses local files instead):
OFFLINE_MODE = True


def get_delays():
    if OFFLINE_MODE:
        delays = pd.read_csv("data/delays/delays-sample.csv")
    else:
        url = (f"https://airlabs.co/api/v9/delays?delay={MIN_DELAY}"
               f"&type=departures&api_key={config.api_key}")
        print(f"Requesting {url}")
        data = requests.get(url).json()
        delays = pd.json_normalize(data['response'])
    return delays


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
    delays.to_csv(f"data/delays/delays-{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv")


if __name__ == '__main__':
    MIN_DELAY = 31  # should be >30
    main()
