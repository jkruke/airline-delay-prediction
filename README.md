# Airline Delay Prediction

This repository contains code for analyzing and predicting the delay time of flights.

## Data collection

Code for data collection is located in [./extract](./extract).
The directory contains script for periodically data fetching and one-time fetching of historical data.

### Local Setup
Copy the [.env.sample](./extract/.env.sample) to `.env` and modify the config values.  
With this file you can override more configuration options like "DELAY_FILE", take a look at the `config.py`. 

## Credits
Thanks to [airlabs.co](https://airlabs.co/) for providing current flight schedules!

Thanks to [Aviation Edge](https://aviation-edge.com/) for providing historical flight schedules!