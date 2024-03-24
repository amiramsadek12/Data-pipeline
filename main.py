import os
import csv
import tempfile
import functions_framework

from dataclasses import dataclass
from typing import List, Tuple, Dict, Union
from dateutil import parser
from google.cloud import storage, bigquery


BUCKET_NAME = "test-modeo-bucket"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "api.json"


@dataclass
class TennisBet:
    """Class representing one row."""

    id: int
    kindred_id: int
    bet_offer_type: int
    has_started: int
    away_odd: float
    home_odd: float
    year: int
    month: int
    day: int
    hour: int
    minute: int

    def __eq__(self, other) -> bool:
        return self.id == other.id

    def to_json(self) -> Dict[str, Union[int, float]]:
        return {
            "id": self.id,
            "kindred_id": self.kindred_id,
            "bet_offer_type": self.bet_offer_type,
            "has_started": self.has_started,
            "away_odd": self.away_odd,
            "home_odd": self.home_odd,
            "year": self.year,
            "month": self.month,
            "day": self.day,
            "hour": self.hour,
            "minute": self.minute,
        }



def extract(blob_name : str) -> List[Tuple]:
    """load the data from the blob in csv format and return it as a list of tuples."""
    result = []
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)

    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as temp_file:
        blob.download_to_filename(temp_file.name)
        with open(temp_file.name, "r") as file:
            cr = csv.reader(file, delimiter=",")
            for row in cr:
                result.append(tuple(row))

    return result[1:]

def remove_duplicates(data: List[TennisBet]) -> List[TennisBet]:
    """Remove duplicates from data."""
    for index, row in enumerate(data):
        for other_row in data[index + 1 :]:
            if row == other_row:
                data.pop(index)
    return data

def transform(raw_data: List[Tuple]) -> List[TennisBet]:
    """Transform the raw data from a list of tuples into a list of class Row."""
    bet_name_mapping = {
        "Match Odds": 0,
        "Set Betting Odds": 1,
        "Game Handicap Odds": 2,
        "Over/Under Total Sets": 3,
    }

    has_started_mapping = {"true": 1, "false": 0}

    row_list = []

    for entry in raw_data:
        date = parser.parse(entry[6]).date()
        time = parser.parse(entry[6]).time()
        entry_as_row = TennisBet(
            id=int(entry[0]),
            kindred_id=int(entry[1]),
            bet_offer_type=bet_name_mapping.get(entry[2]),
            has_started=has_started_mapping.get(entry[3]),
            away_odd=float(entry[4]),
            home_odd=float(entry[5]),
            year=date.year,
            month=date.month,
            day=date.day,
            hour=time.hour,
            minute=time.minute,
        )
        row_list.append(entry_as_row)
    return remove_duplicates(row_list)


def retrieve_dataset_and_table() -> Tuple[bigquery.Dataset, bigquery.Table]:
    """Return dataset and table or create them if they don't exist."""
    client = bigquery.Client()
    dataset_id = "Dataset"
    table_id = "tennis_bet"

    dataset_reference = client.dataset(dataset_id, "test-technique-modeo")

    try:
        dataset = client.get_dataset(dataset_reference)
        table_reference = dataset.table(table_id)
        table = client.get_table(table_reference)
        print("Dataset and table references retrieved.")
        return dataset, table
    except Exception:
        print(f"Dataset {dataset_id} does not exist, creating dataset now.")
        table_schema = [
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("kindred_id", "INTEGER"),
            bigquery.SchemaField("bet_offer_type", "INTEGER"),
            bigquery.SchemaField("has_started", "INTEGER"),
            bigquery.SchemaField("away_odd", "FLOAT"),
            bigquery.SchemaField("home_odd", "FLOAT"),
            bigquery.SchemaField("year", "INTEGER"),
            bigquery.SchemaField("month", "INTEGER"),
            bigquery.SchemaField("day", "INTEGER"),
            bigquery.SchemaField("hour", "INTEGER"),
            bigquery.SchemaField("minute", "INTEGER"),
        ]
        dataset = bigquery.Dataset(dataset_reference)
        dataset.location = "EU"
        table_reference = dataset.table(table_id)
        table = bigquery.Table(table_reference, schema=table_schema)
        try:
            created_dataset = client.create_dataset(dataset)
            created_table = client.create_table(table)
            print(f"Dataset {dataset_id} and table {table_id} have been created.")
            return created_dataset, created_table
        except Exception as e:
            print(f"Dataset creation failed : {e}")


def load(transformed_data: List[TennisBet]) -> None:
    """Load the transformed data into BigQuery.

    Args:
        transformed_data: data we want to save in BigQuery.
    """
    dataset, table = retrieve_dataset_and_table()
    client = bigquery.Client()
    query_to_retrieve_all_ids = f"SELECT id FROM `{dataset.dataset_id}.{table.table_id}`"
    existing_ids = [row[0] for row in client.query(query_to_retrieve_all_ids)]
    rows_to_insert = [row.to_json() for row in transformed_data if row.id not in existing_ids]
    if len(rows_to_insert) == 0:
        print ("No rows to insert.")
        return  
    errors = client.insert_rows_json(table, rows_to_insert)
    if errors == []:
        print(f"{len(rows_to_insert)} rows inserted successfully.")
    else:
        print("Encountered errors while inserting rows:")
        for error in errors:
            print(error)




@functions_framework.cloud_event
def pipeline(cloud_event):
    """entry point of the cloud function when triggered upon creation or update of the blob in our bucket.

    Args:
        cloud_event : the event that would trigger the cloud function.
    """
    file_name = cloud_event.data["name"]
    raw_data = extract(file_name)
    transformed_data = transform(raw_data)
    load(transformed_data)