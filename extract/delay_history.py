import argparse
import json
import os.path
import time
from dataclasses import dataclass
from datetime import datetime, timedelta

import math
import pandas as pd
import requests
import tenacity
from dateutil.relativedelta import relativedelta
from pandas import DataFrame
from tenacity import retry, stop_after_attempt

from config import config
from constants import constants

STATE_FILE = f"{config.data_dir}/history/history-state.json"
DATE_PATTERN = "%Y-%m-%d"


@dataclass
class State:
    latest_date: datetime

    def save_to_file(self):
        with open(STATE_FILE, 'w') as f:
            data = {'latest_date': self.latest_date.strftime(DATE_PATTERN)}
            json.dump(data, f)

    @staticmethod
    def load_from_file():
        if not os.path.isfile(STATE_FILE):
            raise RuntimeError(f"{STATE_FILE} doesn't exist! Consider running --collect first.")

        with open(STATE_FILE, 'r') as f:
            data = json.load(f)

        latest_date = datetime.strptime(data['latest_date'], DATE_PATTERN)
        return State(latest_date=latest_date)


class DelayHistoryProcessor:
    RESULT_CSV = f"{config.data_dir}/history/flightsHistory.csv"
    RAW_DATA_DIR = f"{config.data_dir}/history/flightsHistory_raw"
    INVALID_CSV = f"{config.data_dir}/history/flightsHistory_invalid.csv"
    ETL_STATS_JSON = f"{config.data_dir}/history/etl-stats.json"

    # all civil airports in Vietnam:
    AIRPORTS = ["HAN", "SGN", "BMV", "CXR", "VCA", "HPH", "VCL", "VCS", "DAD", "DIN", "VDH", "TBB", "DLI",
                "HUI", "UIH", "PQC", "PXU", "THD", "VII"]
    etl_stats = {}

    def collect_flights(self, date_from: datetime):
        """
        Collects historical flights from Aviation Edge API and save results to separate JSON files
        """
        day_range = 10
        max_date = self.get_max_date()
        while True:
            date_to = min(date_from + timedelta(days=day_range), max_date)
            print(f"Collecting time range: {date_from} to {date_to}:")
            for t in ["arrival"]:
                for airport in self.AIRPORTS:
                    d_from = date_from.strftime(DATE_PATTERN)
                    d_to = date_to.strftime(DATE_PATTERN)
                    raw_result_file = f"{self.RAW_DATA_DIR}/{d_from}_{d_to}_{airport}_{t}.json"
                    if os.path.isfile(raw_result_file):
                        print(f"{raw_result_file} already exists.")
                        continue

                    url = (f"https://aviation-edge.com/v2/public/flightsHistory?code={airport}&type={t}&"
                           f"date_from={d_from}&date_to={d_to}&key={config.aviation_edge_key}")
                    try:
                        data = self.do_request(url)
                    except Exception as e:
                        print(f"Request to {url} failed: {e}")
                        continue

                    if "error" in data:
                        print(f"Skipping {airport} ({data['error']})")
                        continue

                    with open(raw_result_file, "w") as file:
                        json.dump(data, file)

            if date_to == max_date:
                state = State(latest_date=date_to)
                state.save_to_file()
                print(f"Finished flight collection and saved state {state} to {STATE_FILE}.")
                break
            date_from = date_to + timedelta(days=1)

    @staticmethod
    def get_max_date():
        return (datetime.now() - timedelta(days=4)).replace(hour=0, minute=0, second=0, microsecond=0)

    def etl_flights(self):
        start = time.time()
        # ETL: extract-transform-load
        flights = self.extract_flights()
        flights = self.transform_flights(flights)
        flights = self.clean_flights(flights)
        print("Historical flights after ETL:")
        print(flights)
        flights.sort_values(by="dep_time_utc", ascending=True, inplace=True, ignore_index=True)
        flights.index.name = "Row"
        flights.to_csv(self.RESULT_CSV)

        self.etl_stats["date_from"] = str(flights.iloc[0]["dep_time_utc"])
        self.etl_stats["date_to"] = str(flights.iloc[-1]["dep_time_utc"])
        self.etl_stats["etl_time_s"] = time.time() - start
        with open(self.ETL_STATS_JSON, "w") as f:
            json.dump(self.etl_stats, f, indent=2)

    def extract_flights(self):
        print("\nCurrent stage: EXTRACT\n")
        n_codeshared = 0
        n_total = 0
        dataframes = []
        for f in os.listdir(self.RAW_DATA_DIR):
            raw_file = os.path.join(self.RAW_DATA_DIR, f)
            if os.path.isfile(raw_file):
                with open(raw_file) as rf:
                    data = json.load(rf)
                df = pd.json_normalize(data)
                n_before = len(df)
                n_total += n_before
                if "codeshared.flight.number" in df:
                    df = df[df["codeshared.flight.number"].isnull()]
                n_codeshared += n_before - len(df)
                dataframes.append(df)

        all_flights = pd.concat(dataframes, ignore_index=True)
        print(f"Removed {n_codeshared} code-shared flights")
        n_flights = len(all_flights)
        print(f"Keep {n_flights} out of {n_total} flights ({round(100 * n_flights / n_total)}%)")
        print(all_flights.head(3))
        self.etl_stats["raw"] = n_total
        self.etl_stats["codeshared"] = n_codeshared
        return all_flights

    @retry(stop=stop_after_attempt(3), wait=tenacity.wait_fixed(wait=1))
    def do_request(self, url):
        print(f"Requesting {url}")
        response = requests.get(url, timeout=10)
        return response.json()

    def transform_flights(self, flights: DataFrame):
        print("\nCurrent stage: TRANSFORM\n")
        transformed = pd.DataFrame(columns=constants.target_csv_columns)
        transformed["flight_iata"] = flights["flight.iataNumber"]
        transformed["airline_iata"] = flights["airline.iataCode"]
        transformed["dep_time_utc"] = pd.to_datetime(flights["departure.scheduledTime"])
        transformed["dep_actual_utc"] = pd.to_datetime(flights["departure.actualTime"])
        transformed["arr_time_utc"] = pd.to_datetime(flights["arrival.scheduledTime"])
        transformed["arr_actual_utc"] = pd.to_datetime(flights["arrival.actualTime"])

        transformed["delayed"] = flights["arrival.delay"]

        transformed["arr_iata"] = flights["arrival.iataCode"].str.upper()
        transformed["dep_iata"] = flights["departure.iataCode"].str.upper()
        transformed = self.add_country_codes(transformed)

        transformed["domestic"] = transformed["arr_country_code"] == transformed["dep_country_code"]
        transformed["international"] = ~transformed["domestic"]

        transformed = transformed[constants.target_csv_columns]
        return transformed

    @staticmethod
    def add_country_codes(flights):
        airports = pd.read_json(f"{config.data_dir}/airports.json")
        airports = airports[airports["iata_code"].notnull()]

        flights = pd.merge(flights, airports[["iata_code", "country_code"]], left_on="dep_iata", right_on="iata_code")
        flights = flights.rename(columns={'country_code': 'dep_country_code_x'})

        flights = pd.merge(flights, airports[["iata_code", "country_code"]], left_on="arr_iata", right_on="iata_code")
        flights = flights.rename(columns={'country_code': 'arr_country_code_x'})

        flights["dep_country_code"] = flights["dep_country_code_x"]
        flights["arr_country_code"] = flights["arr_country_code_x"]

        return flights

    def clean_flights(self, flights: DataFrame):
        print("\nCurrent stage: CLEAN\n")

        # remove rows where we don't know if there is any delay:
        valid_flights = flights[(flights["arr_time_utc"].notnull() & flights["arr_actual_utc"].notnull())
                                | flights["delayed"].notnull()]
        n_all_flights = len(flights)
        print(f"Deleted {n_all_flights - len(valid_flights)} of {n_all_flights} rows with unknown delay:")
        invalid_flights = flights[~flights.index.isin(valid_flights.index)]
        print(invalid_flights)
        invalid_flights.to_csv(self.INVALID_CSV, index=False)
        self.etl_stats["missing_information"] = len(invalid_flights)

        valid_flights.reset_index(drop=True, inplace=True)
        flights = valid_flights.copy()

        def calc_delay(row):
            if math.isnan(float(row['delayed'])):
                return int((row['arr_actual_utc'] - row['arr_time_utc']).total_seconds() / 60)
            return row['delayed']
        # calculate the delay in minutes and assign it to 'delayed' where it's missing
        flights['delayed'] = flights.apply(calc_delay, axis=1)

        print("Removing duplicates, keep the last duplicate which probably contains the most recent times")
        n_flights = len(flights)
        flights.drop_duplicates(subset=["flight_iata", "dep_time_utc"],
                                ignore_index=True, keep='last', inplace=True)
        n_duplicates = n_flights - len(flights)
        print(f"Removed {n_duplicates} duplicates")
        self.etl_stats["duplicate_iata_dep_time"] = n_duplicates

        return flights


def main():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', 20)
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-m", "--mode", choices=["collect", "etl", "update"], required=True,
                            help="Execution mode"
                                 "collect: request API for largest possible history and save to JSON files."
                                 "etl: do ETL based on the JSON files."
                                 "update: do 'collect' + 'etl' for the most recent unseen history.")
    args = arg_parser.parse_args()
    print(f"program arguments: {args}")
    processor = DelayHistoryProcessor()
    if args.mode == "collect":
        # collect the maximum possible history
        date_from = datetime.now() - relativedelta(years=1) + timedelta(days=1)
        processor.collect_flights(date_from=date_from)
    elif args.mode == "etl":
        processor.etl_flights()
    elif args.mode == "update":
        state = State.load_from_file()
        if state.latest_date >= processor.get_max_date():
            print(f"Already up to date (latest data from {state.latest_date})")
            return

        date_from = state.latest_date + relativedelta(days=1)
        processor.collect_flights(date_from=date_from)
        processor.etl_flights()
    else:
        print("NOTHING TO DO! Run with --help for information about this program.")


if __name__ == '__main__':
    main()
