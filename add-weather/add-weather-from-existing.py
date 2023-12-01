#!/usr/bin/python3

import pandas as pd
from pandasql import sqldf	# Because working with sql is way simpler 

import datetime
from argparse import ArgumentParser

parser = ArgumentParser(prog='add-weather-from-existing', description='Append informations from @weather_csv to flight @delays_csv data and write it to @output_csv.', epilog='Example usage: $> ./add-weather-from-existing.py all_weathers.csv flightHistory-with-weekday_holiday.csv o.csv')
parser.add_argument("weather", help="CSV file of weather reports.", metavar='weather_csv')
parser.add_argument("delays", help='A CSV file of flight delays.', metavar='delays_csv')
parser.add_argument("output", help='Output CSV file.', metavar='output_csv')
args = parser.parse_args()

print("Reading @delays data frame")
delays = pd.read_csv(args.delays, header=0)
delays = delays.head(10)	# Debugging: Test whether the information is correct
print(">Done")

print("Reading @weather data frame")
weather = pd.read_csv(args.weather, header=0)
print(">Done")

print("Renaming arrival/departure weather columns")
arrival_weather = sqldf("""SELECT iata, date,
							temperature_2m AS arr_temperature_2m,
							relative_humidity_2m AS arr_relative_humidity_2m,
							precipitation AS arr_precipitation,
							rain AS arr_rain,
							snowfall AS arr_snowfall,
							weather_code AS arr_weather_code,
							cloud_cover AS arr_cloud_cover,
							cloud_cover_low AS arr_cloud_cover_low,
							cloud_cover_mid AS arr_cloud_cover_mid,
							cloud_cover_high AS arr_cloud_cover_high,
							wind_speed_10m AS arr_wind_speed_10m,
							wind_speed_100m AS arr_wind_speed_100m,
							wind_gusts_10m AS arr_wind_gusts_10m FROM weather""")

departure_weather = sqldf("""SELECT iata, date,
							temperature_2m AS dep_temperature_2m,
							relative_humidity_2m AS dep_relative_humidity_2m,
							precipitation AS dep_precipitation,
							rain AS dep_rain,
							snowfall AS dep_snowfall,
							weather_code AS dep_weather_code,
							cloud_cover AS dep_cloud_cover,
							cloud_cover_low AS dep_cloud_cover_low,
							cloud_cover_mid AS dep_cloud_cover_mid,
							cloud_cover_high AS dep_cloud_cover_high,
							wind_speed_10m AS dep_wind_speed_10m,
							wind_speed_100m AS dep_wind_speed_100m,
							wind_gusts_10m AS dep_wind_gusts_10m FROM weather""")
print(">Done")


print("Append weather info to arrival flights")
delays = sqldf(f'''SELECT *
				FROM delays AS d JOIN arrival_weather as a
				ON d.arr_iata = a.iata 
					AND unixepoch(a.date) <= unixepoch(d.arr_time_utc) 
			   		AND unixepoch(d.arr_time_utc) < (unixepoch(a.date) + 3600)''')
print(delays)


print("Append weather info to departure flights")
delays = sqldf(f'''SELECT *
				FROM delays AS d JOIN departure_weather AS dep
				ON d.dep_iata = dep.iata 
					AND unixepoch(dep.date) <= unixepoch(d.dep_time_utc) 
			   		AND unixepoch(d.dep_time_utc) < (unixepoch(dep.date) + 3600)''')
print(delays)
print(">Done")

delays.to_csv(args.output)