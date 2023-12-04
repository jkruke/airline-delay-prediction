#!/usr/bin/env bash

# fetch new flight data from the API and save to extract/data/flightsHistory.csv
#cd extract && python delay_history.py -m update && cd ..

# add holiday information and save to extract/data/history/flightsHistory-holiday.csv
python add-holidays/add_holidays.py extract/data/history/flightsHistory.csv

# add weather information from all_weathers.csv and save to flightsHistory-weather.csv
python add-weather/add-weather-from-existing.py add-weather/all_weathers.csv extract/data/history/flightsHistory-holidays.csv extract/data/history/flightsHistory-weather.csv