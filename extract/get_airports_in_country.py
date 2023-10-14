import requests

from config import config


def main():
    url = f"https://airlabs.co/api/v9/airports?country_code={config.country}&api_key={config.api_key}"
    data = requests.get(url).json()
    airports = [e["iata_code"] for e in data["response"] if e["iata_code"]]
    print(",".join(airports))


if __name__ == '__main__':
    main()
