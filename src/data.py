import pandas as pd
import requests
from tqdm import tqdm
from pathlib import Path
from typing import Optional, List
from src.paths import RAW_DATA_DIR

def download_data_file(year: int, month: int)->Path:
    """
    Downloads a file from url and stores it locally
    """
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet'
    response = requests.get(url=url)
    
    if response.status_code == 200:
        DESTINATION_PATH = RAW_DATA_DIR/Path(f"rides_{year}_{month:02d}.parquet")
        DESTINATION_PATH.parent.mkdir(parents=True, exist_ok=True)

        raw_data = response.content
        with open(DESTINATION_PATH, 'wb') as f:
            f.write(raw_data)
        return DESTINATION_PATH

    else:
        raise Exception(f"{url} doesn't exist.")

def validate_raw_data(data:pd.DataFrame, year:int, month:int)->pd.DataFrame:
    first_day_month = f"{year}-{month:02d}-01"
    next_month_start=f"{year}-{1+month:02d}-01" if month <12 else f"{year+1}-01-01"
    data = data[data.pickup_datetime >= first_day_month]
    data = data[data.pickup_datetime < next_month_start]
    return data

def load_raw_data(year:int, months: Optional[List[int]]=None)->pd.DataFrame:
    data_df = pd.DataFrame()
    if months is None:
        # Download data for all months
        months = range(1, 13, 1)
    for month in tqdm(months):
        raw_path = download_data_file(year = year, month=month)
        # load the parquet file as df
        rides_one_month = pd.read_parquet(raw_path)
        # Subset columns for time pickup and location and rename
        keep_columns = ["tpep_pickup_datetime", "PULocationID"]
        rides = rides_one_month[keep_columns]
        rides.rename(columns = {
            "tpep_pickup_datetime":"pickup_datetime", 
            "PULocationID":"location_id"}, inplace=True)

        # Validate the monthly data
        validated_rides = validate_raw_data(rides, year=year, month=month)
        data_df = pd.concat([data_df, validated_rides])
    data_df.reset_index(drop=True)

    return data_df