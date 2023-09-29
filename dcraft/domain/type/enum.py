from enum import Enum
from typing import List

import pandas as pd


class ContentType(Enum):
    DF = pd.DataFrame
    DICT = dict
    DICT_LIST = List[dict]


class Extension(Enum):
    JSON = "json"
    CSV = "csv"
    PARQUET = "parquet"
