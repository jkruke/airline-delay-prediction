import os
from dataclasses import dataclass

import dotenv


@dataclass
class Config:
    api_key: str
    aviation_edge_key: str
    country: str
    delay_type: str
    output_dir: str
    data_dir: str
    output_filename: str
    min_delay: int
    error_download_retries: int
    error_download_wait_time: int


dotenv.load_dotenv()
config = Config(
    api_key=os.getenv("API_KEY", "no_key"),
    aviation_edge_key=os.getenv("AVIATION_EDGE_KEY", "no_key"),
    output_dir=os.getenv("output_dir", "no_file"),
    data_dir=os.getenv("DATA_DIR", "data"),
    output_filename="delays.csv",
    country="VN",
    delay_type="departures", # or "arrivals",
    min_delay=31,
    error_download_retries=3,
    error_download_wait_time=20)
