from argparse import ArgumentParser

import pandas as pd

from config import config


def main():
    arg_parser = ArgumentParser()
    arg_parser.add_argument("FLIGHTS_CSV", help="Provide the CSV file containing the flight history")
    args = arg_parser.parse_args()
    print(f"Program arguments: {args}")
    flights = pd.read_csv(args.FLIGHTS_CSV)
    airports_dep = flights["dep_iata"].drop_duplicates(ignore_index=True)
    airports_arr = flights["arr_iata"].drop_duplicates(ignore_index=True)
    airports = pd.concat([airports_dep, airports_arr], ignore_index=True).drop_duplicates(ignore_index=True)
    print(airports)
    airports.to_csv(f"{config.data_dir}/affected-airports.csv", index=False, header=False)


if __name__ == '__main__':
    main()