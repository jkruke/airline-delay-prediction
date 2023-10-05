import os
from dataclasses import dataclass

import dotenv


@dataclass
class Config:
    api_key: str
    min_delay: int
    country: str


dotenv.load_dotenv()
config = Config(api_key=os.getenv("API_KEY"),
                min_delay=int(os.getenv("MIN_DELAY", "31")),
                country=os.getenv("COUNTRY", "VN"))
