from datetime import datetime

import pandas as pd
import requests

from extract.config import config


def main():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    url = (f"https://airlabs.co/api/v9/delays?delay={MIN_DELAY}&dep_iata={config.airports}"
           f"&type=departures&api_key={config.api_key}")
    print(f"Requesting {url}")
    data = requests.get(url).json()
    df = pd.json_normalize(data['response'])
    print(f"Result: {df}")
    df.to_csv(f"data/delays/delays-{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv")


if __name__ == '__main__':
    MIN_DELAY = 31  # should be >30
    main()
