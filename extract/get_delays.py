import requests
import pandas as pd
from extract.config import config


def main():
    print(config.airports)
    url = (f"https://airlabs.co/api/v9/delays?delay={MIN_DELAY}&dep_iata={config.airports}"
           f"&type=departures&api_key={config.api_key}")
    data = requests.get(url).json()
    df = pd.to_json(data)
    print(df)


if __name__ == '__main__':
    MIN_DELAY = 31  # should be >30
    main()
