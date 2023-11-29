from argparse import ArgumentParser

import pandas as pd

# information from: https://www.timeanddate.com/holidays/vietnam/2023
VN_HOLIDAYS = {
    "2022-10-20": "Vietnamese Women's Day",
    "2022-10-31": "Halloween",
    "2022-12-22": "Solstice",
    "2022-12-24": "Christmas Eve",
    "2022-12-25": "Christmas Day",
    "2022-12-31": "International New Year's Eve",
    "2023-01-01": "International New Year's Day",
    "2023-01-02": "Day off for International New Year's Day",
    "2023-01-20": "Lunar New Year",
    "2023-01-21": "Lunar New Year",
    "2023-01-22": "Lunar New Year",
    "2023-01-23": "Lunar New Year",
    "2023-01-24": "Lunar New Year",
    "2023-01-25": "Lunar New Year",
    "2023-01-26": "Lunar New Year",
    "2023-02-14": "Valentine's Day",
    "2023-03-21": "March Equinox",
    "2023-04-09": "Easter Sunday",
    "2023-04-29": "Hung Kings Festival",
    "2023-04-30": "Liberation/Reunification Day",
    "2023-05-01": "Labor Day",
    "2023-05-02": "Day off Hung Kings Festival",
    "2023-05-03": "Day off Liberation/Reunification Day",
    "2023-05-05": "Vesak",
    "2023-06-21": "June Solstice",
    "2023-06-28": "Vietnamese Family Day",
    "2023-09-01": "Independence Day Holiday",
    "2023-09-02": "Independence Day",
    "2023-09-03": "Independence Day Holiday",
    "2023-09-04": "Independence Day observed",
    "2023-09-23": "Equinox",
    "2023-10-20": "Vietnamese Women's Day",
    "2023-10-31": "Halloween",
    "2023-12-22": "Solstice",
    "2023-12-24": "Christmas Eve",
    "2023-12-25": "Christmas Day",
    "2023-12-31": "International New Year's Eve"
}


def get_input_file():
    arg_parser = ArgumentParser()
    arg_parser.add_argument("FLIGHTS_CSV", help="Provide the CSV file containing the flight history")
    args = arg_parser.parse_args()
    print(f"Program arguments: {args}")
    return args.FLIGHTS_CSV


def load_flights():
    input_file = get_input_file()
    flights = pd.read_csv(input_file)
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
    flights['holiday'] = flights['arr_time_utc'].map(get_holiday)
    flights.info()
    print(flights.head())
    output_file = get_input_file()
    output_file = output_file.replace(".csv", "-holidays.csv")
    flights.to_csv(output_file, header=True, index=False)


if __name__ == '__main__':
    main()
