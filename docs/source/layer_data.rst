==================================================
Layer Data
==================================================

dcraft's data management is based on the concept of layers. The data are stored on each layers for each purposes.  
The purpose of each layers are like below.  

| **Raw Layer Data**: Hold the source data as they are
| **Trusted Layer Data**: Hold the cleaned data
| **Refined Layer Data**: Hold the processed data for each purposes 

To compose the layer's data, there are two ways, load from storage or create.

By giving the metadata's id, you can load the data and store on the corresponding object.

.. code-block:: python

    from dcraft import read_layer_data
    from dcraft import LocalDataRepository, LocalMetadataRepository

    data_repository = LocalDataRepository(DATA_DIR_PATH)
    metadata_repository = LocalMetadataRepository(DATA_DIR_PATH)

    loaded_raw_layer_data = read_layer_data("<id-from-metadata>", data_repository, metadata_repository)

   

To create, for each layer, there are functions.  

.. code-block:: python

    from dcraft import create_raw
    import pandas as pd

    data = pd.DataFrame({"a": [1,2], "b": [None, 4]})
    raw_layer_data = create_raw(
        data,
        "fake-project"
    )


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   code_generated/dcraft.domain.layer