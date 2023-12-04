#!/usr/bin/python3
import time

import pandas as pd

from argparse import ArgumentParser

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_rows', 20)

parser = ArgumentParser(prog='add-weather-from-existing', description='Merges informations from @weather_csv with @flights_csv data and write it to @output_csv.',
                        epilog='Example usage: $> ./add-weather-from-existing.py all_weathers.csv flightHistory-with-weekday_holiday.csv o.csv')
parser.add_argument("weather", help="CSV file of weather reports.", metavar='weather_csv')
parser.add_argument("flights", help='A CSV file of flight delays.', metavar='flights_csv')
parser.add_argument("output", help='Output CSV file.', metavar='output_csv')
args = parser.parse_args()

print("Reading @flights data frame")
flights = pd.read_csv(args.flights, header=0, parse_dates=["arr_time_utc", "dep_time_utc"])
print(">Done")

print("Reading @weather data frame")
weather = pd.read_csv(args.weather, header=0, parse_dates=True, index_col=["date", "iata"],
                      date_format="%Y-%m-%d %H:%M:%S.%f")
weather.drop(columns=["Unnamed: 0"], inplace=True)
print(">Done")

print("Renaming arrival/departure weather columns")
arrival_weather = weather.add_prefix("arr_")
departure_weather = weather.add_prefix("dep_")
print(">Done")
# release memory:
del weather

print("Adding columns with hour-rounded time")
flights["arr_hour"] = flights["arr_time_utc"].dt.round("H")
flights["dep_hour"] = flights["dep_time_utc"].dt.round("H")

print("Append weather info to arrival flights")
start = time.time()
flights = pd.merge(flights, arrival_weather, left_on=["arr_hour", "arr_iata"], right_index=True, how="left")
print(flights)
print(f">Done ({time.time() - start}s)")

print("Append weather info to departure flights")
start = time.time()
flights = pd.merge(flights, departure_weather, left_on=["dep_hour", "dep_iata"], right_index=True, how="left")
print(flights)
print(f">Done ({time.time() - start}s)")

print("Drop intermediate columns")
start = time.time()
flights.drop(columns=["arr_hour", "dep_hour"], inplace=True)
print(f">Done ({time.time() - start}s)")

print(f"Write data to {args.output}")
flights.to_csv(args.output, index=False)
print(">Done")
