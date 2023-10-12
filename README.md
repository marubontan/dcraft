# dcraft [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) ![deployment workflow](https://github.com/marubontan/dcraft/actions/workflows/publish-to-pypi.yml/badge.svg)
Data management library based on data lake concept especially for data science and machine leaning.  
This helps your daily job's data management by raw, trusted and refined layer concept from data lake. The data is versioned and saved for each layer.  
## Installation
```
pip install dcraft
```
To use GCP resources.  
```
pip install dcraft[gcp]
```
## Concept
For daily individual work and for team work, we need to manage and organize our datasets to keep clean workflow. This library is to help that based on the data lake's layer concept.  
```
dataset -> RawDataLayer -(cleaning and validation)-> TrustedDataLayer -(optimized for use case)-> RefinedDataLayer
```
For each layer, you can save the data and metadata to several places such as local file system and GCP.  

## Covered Storage and Table
You can save the metadata and data on several places. The list below is the present coverage.  

### Metadata
* Local File System
* BigQuery

### Data
* Local File System
* Google Cloud Storage

## Example
Create layer's data. There are `create_trusted` and `create_refined` too.  
```python
from dcraft import create_raw
import pandas as pd

data = pd.DataFrame({"a": [1,2], "b": [None, 4]})
raw_layer_data = create_raw(
    data,
    "fake-project",
    "Shuhei Kishi",
    "This is fake project",
    {"version": "0.0.1"}
)
```
You can choose where the data and metadata should be saved. On this example, it saves both on local.  
```python
import os
from dcraft import LocalDataRepository, LocalMetadataRepository

CURRENT_DIR = os.getcwd()
DATA_DIR_PATH = os.path.join(CURRENT_DIR, "data")
METADATA_DIR_PATH = os.path.join(CURRENT_DIR, "metadata")

data_repository = LocalDataRepository(DATA_DIR_PATH)
metadata_repository = LocalMetadataRepository(DATA_DIR_PATH)
raw_layer_data.save("parquet", data_repository, metadata_repository)
```
The data was saved to raw layer and information were saved as metadata.  
You can read the saved data from metadata's id. The format is kept.  
```python
from dcraft import read_layer_data
loaded_raw_layer_data = read_layer_data(<id-from-metadata>, data_repository, metadata_repository)
```
If you want to save the metadata and data on different places such as BigQuery and Google Cloud Storage, you can use different `Repository` class.  
```python
from dcraft import BqMetadataRepository, GcsDataRepository

GCP_PROJECT = "your-project-id"
GCS_BUCKET = "your-bucket-name"

data_repository = GcsDataRepository(GCP_PROJECT, GCS_BUCKET)
metadata_repository = BqMetadataRepository(GCP_PROJECT, "test_dataset", "test_table")

raw_layer_data.save("csv", data_repository, metadata_repository)

```
