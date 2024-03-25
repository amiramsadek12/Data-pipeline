# Modeo

This project is a data pipeline that automates the extraction, transformation, and loading of tennis betting data from a CSV file into a BigQuery table. The process is triggered by the upload of the CSV file to a Google Cloud Storage bucket.

## Overview

The data pipeline consists of two main components:
- **Main Script (`main.py`)**: This script contains functions for extracting, transforming, and loading the data. It defines a `TennisBet` class to represent each row of the dataset and includes functions to extract the data from the CSV file, transform it into the desired format, and load it into a BigQuery table.
- **Upload Script (`upload_data_to_blob_storage.py`)**: This script is responsible for uploading a CSV file from the local filesystem to a Google Cloud Storage bucket.

## How It Works

1. **CSV Upload*![Uploading Diagramme sans nom.drawio (2).png…]()
*: A CSV file containing raw tennis betting data is uploaded to a specified Google Cloud Storage bucket using the `upload_data_to_blob_storage.py` script.

2. **Cloud Storage Trigger**: The upload of the CSV file to the Cloud Storage bucket triggers a Cloud Function.

3. **Cloud Function Execution**: The Cloud Function, defined in `main.py`, is executed in response to the Cloud Storage event. The function's entry point, `pipeline`, is invoked.

4. **Data Processing**: The `pipeline` function extracts the raw data from the uploaded CSV file, transforms it into the desired format using the `TennisBet` class and associated functions, and loads it into a BigQuery table.

## Project Structure

- `main.py`: Contains the main script for data extraction, transformation, and loading.
- `upload_data_to_blob_storage.py`: Script responsible for uploading CSV files to Google Cloud Storage.
- `api.json`: JSON file containing authentication credentials for accessing Google Cloud APIs.
- `requirements.txt`: File listing the Python dependencies required for the project.

## Usage

To use this project, follow these steps:

1. Clone the repository to your local machine:
[Project Repository](https://github.com/amiramsadek12/Modeo)

2. Install the required Python dependencies:
```pip install -r requirements.txt```

3. Copy the `api.json` file to the project directory so the scripts can work.

4. Run the `upload_data_to_blob_storage.py` script to upload a CSV file to the specified Google Cloud Storage bucket:
```python upload_data_to_blob_storage.py --source_file_name path/to/your/csv/file.csv```

![pipeline diagram](https://github.com/amiramsadek12/Modeo/assets/125670249/f741e749-4332-4696-b818-3d2b0c6508d0)
