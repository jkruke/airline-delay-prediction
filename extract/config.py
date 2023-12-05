import os
from dataclasses import dataclass

import dotenv


@dataclass
class Config:
    api_key: str
    aviation_edge_key: str
    country: str
    data_dir: str


dotenv.load_dotenv()
config = Config(
    api_key=os.getenv("API_KEY", "no_key"),
    aviation_edge_key=os.getenv("AVIATION_EDGE_KEY", "no_key"),
    data_dir=os.getenv("DATA_DIR", "../data"),
    country="VN")
