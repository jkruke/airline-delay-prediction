import math
from datetime import datetime, timedelta

import pandas as pd
import requests
from pandas import DataFrame

from config import config
from constants import constants


class DelayHistoryFetcher:
    # all civil airports in Vietnam:
    airports = ["BMV", "CAH", "CXR", "VCA", "HPH", "VCL", "VCS", "DAD", "DIN", "VDH", "TBB", "KON", "DLI", "SQH", "NHA",
                "HOO", "HAN", "PHA", "HUI", "UIH", "PQC", "PHU", "VSO", "PXU", "XNG", "VKG", "SOA", "TMK", "SGN", "THD",
                "VDO", "VII", "VTG"]
    date_from = "2023-10-01"
    date_to = (datetime.now() - timedelta(days=4)).strftime("%Y-%m-%d")

    def fetch(self):
        # ETL: extract-transform-load
        flights = self.extract_flights()
        flights = self.transform_flights(flights)
        flights = self.clean_flights(flights)
        print("Historical flights after ETL:")
        print(flights)
        flights.to_csv("data/history/flightsHistory.csv", index=False)

    def extract_flights(self):
        print("\nCurrent stage: EXTRACT\n")
        dataframes = []
        for t in ["arrival", "departure"]:
            for airport in self.airports:
                url = (f"https://aviation-edge.com/v2/public/flightsHistory?code={airport}&type={t}&"
                       f"date_from={self.date_from}&key={config.aviation_edge_key}&date_to={self.date_to}")
                print(f"Requesting {url}")
                response = requests.get(url)
                data = response.json()
                if "error" in data:
                    print(f"Skipping {airport} ({data['error']})")
                    continue

                airport_flights = pd.json_normalize(data)
                print(f"Found {len(airport_flights)} flights with {t} at {airport}")
                dataframes.append(airport_flights)
        all_flights = pd.concat(dataframes, ignore_index=True)
        print("All flights:")
        print(all_flights)
        return all_flights

    def transform_flights(self, flights: DataFrame):
        print("\nCurrent stage: TRANSFORM\n")
        transformed = pd.DataFrame(columns=constants.target_csv_columns)
        transformed["flight_iata"] = flights["flight.iataNumber"]
        transformed["airline_iata"] = flights["airline.iataCode"]
        transformed["dep_time_utc"] = flights["departure.scheduledTime"]
        transformed["dep_estimated_utc"] = flights["departure.actualTime"]
        transformed["arr_time_utc"] = flights["arrival.scheduledTime"]
        transformed["arr_estimated_utc"] = flights["arrival.actualTime"]

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
        airports = pd.read_json("data/airports.json")
        airports = airports[airports["iata_code"].notnull()]

        flights = pd.merge(flights, airports[["iata_code", "country_code"]], left_on="dep_iata", right_on="iata_code")
        flights = flights.rename(columns={'country_code': 'dep_country_code_x'})

        flights = pd.merge(flights, airports[["iata_code", "country_code"]], left_on="arr_iata", right_on="iata_code")
        flights = flights.rename(columns={'country_code': 'arr_country_code_x'})

        flights["dep_country_code"] = flights["dep_country_code_x"]
        flights["arr_country_code"] = flights["arr_country_code_x"]

        return flights

    @staticmethod
    def clean_flights(flights: DataFrame):
        print("\nCurrent stage: CLEAN\n")

        # remove rows where we don't know if there is any delay:
        valid_flights = flights[(flights["dep_estimated_utc"].notnull() & flights["arr_estimated_utc"].notnull())
                                | flights["delayed"].notnull()]
        n_all_flights = len(flights)
        print(f"Deleted {n_all_flights - len(valid_flights)} of {n_all_flights} rows with unknown delay:")
        invalid_flights = flights[~flights.index.isin(valid_flights.index)]
        print(invalid_flights)
        invalid_flights.to_csv("data/history/flightsHistory_invalid.csv", index=False)

        valid_flights.reset_index(drop=True, inplace=True)
        flights = valid_flights

        # calculate delay if not already provided
        flights['arr_estimated_utc'] = pd.to_datetime(flights['arr_estimated_utc'])
        flights['arr_time_utc'] = pd.to_datetime(flights['arr_time_utc'])
        # Calculate the difference in minutes and assign it to 'delayed' where it's missing
        flights['delayed'] = flights.apply(lambda row: int((row['arr_estimated_utc'] - row['arr_time_utc'])
                                                           .total_seconds() / 60)
                                                     if math.isnan(float(row['delayed'])) else row['delayed'], axis=1)

        return flights


if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', 20)
    DelayHistoryFetcher().fetch()
