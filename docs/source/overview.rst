==================================================
Overview
==================================================

dcraft is data management library. It helps you to manage the dataset based on the idea of datalake layers.  

***************
Detail
***************
On experiments and development, it is always important to manage the data properly. 
But it is not easy. There are several data sources and you need to process them in several steps and for several purposes. 
dcraft is to do this management.
| dcraft is using the same concept as data lake layers for the data management. 
**Raw data layer** is to store the source data as it is. **Trusted data layer** is to store the cleaned, validated and transformed data which are from **Raw data layer**. 
**Refined data layer** is to store the processed data for each specific use case and purposes.  
| Your can save, load and track the linked data by dcraft. Each layer's data are saved on specified repositories. And based on the metadata, you can load easily.


***************
Use case
***************
Imagine the situation that you have a data scinece project. The data are extracted from the several tables and DB. 
You have several tasks and for each, you need to process those data.
| Of course, even for the same task, sometimes, you need to update the processor or query. And it is preferable to manage the version of the data. 
| dcraft will help this case.

For example, your project is using company's internal data and some free data on the internet.
| Those data can be save independently to the **Raw data layer**. You can do some cleaning and validation processes to those and save each of them to **Trusted data layer**. 
For each specific tasks, you can merge and process the **Trusted data layer**'s data and save to the **Refined data layer**.

With dcraft, each data are managed by version. And you can easily load.

***************
To start
***************

.. code-block::

    pip install dcraft



***************
Examples
***************
From the source data, create raw layer data.  

.. code-block:: python

    from dcraft import create_raw
    import pandas as pd

    data = pd.DataFrame({"a": [1,2], "b": [None, 4]})
    raw_layer_data = create_raw(
        data,
        "fake-project"
    )

   
This example stores metadata and data on the local file system.  

.. code-block:: python

    import os
    from dcraft import LocalDataRepository, LocalMetadataRepository

    CURRENT_DIR = os.getcwd()
    DATA_DIR_PATH = os.path.join(CURRENT_DIR, "data")
    METADATA_DIR_PATH = os.path.join(CURRENT_DIR, "metadata")

    data_repository = LocalDataRepository(DATA_DIR_PATH)
    metadata_repository = LocalMetadataRepository(DATA_DIR_PATH)
    raw_layer_data.save("parquet", data_repository, metadata_repository)



.. code-block:: python

    from dcraft import read_layer_data
    loaded_raw_layer_data = read_layer_data("<id-from-metadata>", data_repository, metadata_repository)


For trusted and refined, you can do same.  

.. code-block:: python

    from dcraft import create_trusted, create_refined

    trusted_content = raw_layer_data.content.dropna(axis=0)
    trusted_layer_data = create_trusted(
        trusted_content,
        "fake-project",
        "Shuhei Kishi",
        "This is fake project",
        {"version": "0.0.1"},
        [raw_layer_data.id]
    )
    trusted_layer_data.save("csv", data_repository, metadata_repository)

    refined_content = trusted_layer_data.content * 2
    refined_layer_data = create_refined(
        content,
        "fake-project",
        "Shuhei Kishi",
        "This is fake project",
        {"version": "0.0.1"},
        source_ids=[trusted_layer_data.id]
    )
