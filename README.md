# Airline Delay Prediction

This repository contains code for analyzing and predicting the delay time of flights.

## Data Collection

Code for data collection is located in [./extract](./extract).
The directory contains script for periodically data fetching and one-time fetching of historical data.

### Historical Data
Historical flight schedule and delay information are fetched from the Airlabs Edge API (see Credits) with 
[delay_history.py](./extract/delay_history.py).  
Execute `python extract/delay_history.py --help` for more information about the program.

As a result, we get historic flight data of (now - 1 year) for flights with departure or arrival at a airport located
in Vietnam. 
Only civil airports with a known IATA code are considered. 
However, some of them are excluded because of unavailable data.  
Included airports: `HAN, SGN, BMV, CXR, VCA, HPH, VCL, VCS, DAD, DIN, VDH, TBB, DLI, HUI, UIH, PQC, PXU, THD, VII`  
Excluded airports: `CAH, KON, SQH, NHA, HOO, PHA, PHU, VSO, XNG, VKG, SDA, VDO, VTG`

### Current Data
Current flight delay data are fetched from the airlabs API (see Credits) with [get_delays.py](./extract/get_delays.py).

### Local Setup
Copy the [.env.sample](./extract/.env.sample) to `.env` and modify the config values.  
With this file you can override more configuration options like "DELAY_FILE", take a look at the `config.py`. 

## Credits
Thanks to [airlabs.co](https://airlabs.co/) for providing current flight schedules!

Thanks to [Aviation Edge](https://aviation-edge.com/) for providing historical flight schedules!