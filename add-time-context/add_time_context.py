import datetime
from argparse import ArgumentParser

import numpy as np
import pandas as pd

# information from: https://www.timeanddate.com/holidays/vietnam/2023
VN_HOLIDAYS = {
    "2023-01-01": "New Year 1.1.",
    "2023-01-20": "Lunar New Year",
    "2023-01-21": "Lunar New Year",
    "2023-01-22": "Lunar New Year",
    "2023-01-23": "Lunar New Year",
    "2023-01-24": "Lunar New Year",
    "2023-01-25": "Lunar New Year",
    "2023-01-26": "Lunar New Year",
    "2023-04-29": "Hung Kings Festival",
    "2023-04-30": "Liberation/Reunification Day",
    "2023-05-01": "Labor Day",
    "2023-05-02": "Day off Hung Kings Festival",
    "2023-05-03": "Day off Liberation/Reunification Day",
    "2023-09-01": "Independence Day Holiday",
    "2023-09-02": "Independence Day",
    "2023-09-03": "Independence Day Holiday",
    "2023-09-04": "Independence Day observed",
}


def load_flights():
    arg_parser = ArgumentParser()
    arg_parser.add_argument("FLIGHTS_CSV", help="Provide the CSV file containing the flight history")
    args = arg_parser.parse_args()
    print(f"Program arguments: {args}")
    flights = pd.read_csv(args.FLIGHTS_CSV)
    for col in ["dep_time_utc", "dep_actual_utc", "arr_time_utc", "arr_actual_utc"]:
        flights[col] = pd.to_datetime(flights[col])
    return flights


def get_holiday(time_dt):
    date = time_dt.strftime("%Y-%m-%d")
    if date in VN_HOLIDAYS:
        return VN_HOLIDAYS[date]
    return None


def main():
    flights = load_flights()
    flights['weekday'] = flights['arr_time_utc'].dt.day_name()
    flights['holiday'] = flights['arr_time_utc'].map(get_holiday)
    flights.info()
    print(flights.head())
    flights.to_csv("flightHistory-with-weekday_holiday.csv", header=True, index=False)


if __name__ == '__main__':
    main()
