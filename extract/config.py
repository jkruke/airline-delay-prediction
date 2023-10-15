import os
from dataclasses import dataclass

import dotenv


@dataclass
class Config:
    api_key: str
    aviation_edge_key: str
    country: str
    delay_type: str
    delay_file: str
    min_delay: int
    error_download_retries: int
    error_download_wait_time: int


dotenv.load_dotenv()
config = Config(
    api_key=os.getenv("API_KEY"),
    aviation_edge_key=os.getenv("AVIATION_EDGE_KEY"),
    country=os.getenv("COUNTRY", "VN"),
    delay_type=os.getenv("delay_type", "departures"),
    delay_file=os.getenv("delay_file", "data/delays/delays.csv"),
    min_delay=int(os.getenv("min_delay", 31)),
    error_download_retries=int(os.getenv("EDR", 3)),
    error_download_wait_time=int(os.getenv("EDWT", 20)))
