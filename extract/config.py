import os
from dataclasses import dataclass

import dotenv


@dataclass
class Config:
    api_key: str
    country: str


dotenv.load_dotenv()
config = Config(api_key=os.getenv("API_KEY"),
                country=os.getenv("COUNTRY", "VN"))
