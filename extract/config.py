import os
from dataclasses import dataclass

import dotenv


@dataclass
class Config:
    api_key: str
    aviation_edge_key: str
    country: str


dotenv.load_dotenv()
config = Config(api_key=os.getenv("API_KEY"),
                aviation_edge_key=os.getenv("AVIATION_EDGE_KEY"),
                country=os.getenv("COUNTRY", "VN"))
