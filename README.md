# Ingestion, ETL, and stream processing pipelines with Azure Databricks and Delta Lake

## Project Description

This is a small project that I did in the process of learning the subject **Cloud Computing** at UIT. In this one, I built simple pipelines for batch and stream 
processing using **Event Hubs**, **Data Factory**, **Azure Databricks**, and **Delta Lake** based on the architecture mentioned below.

## Architecture 

![pipelines](https://github.com/ngochien1007/yellow-tripdata/assets/154615929/48fc1c15-9560-44a0-8d2e-7f1401b1297f)

## Tutorial

**Step 1**: Using `preprocessing.ipynb` to split the original data file to 2 files for demonstration: one simulated file for batch processing is **Batch_Data.csv** and one simulated file for streaming processing is **Streaming_Data.csv**.

**Step 2**: In Microsoft Azure, create a resource group with essential marketplaces such as Storage Account, Event Hubs, Data Factory, and Databricks.

**Step 3**: In Data Factory, create a pipeline to ingest and process batch data. Here, **Batch_Data.csv** is divided into 2 partitions to simulate the actual case when there is new data or a time period when data needs to be updated according to the process.

**Step 4**: Connect Databricks to Data Lake Storage Gen2.

**Step 5**: Collect streaming data using Python with `send.py` and `ETL_EventHubs_Databricks_Storage.ipynb`.

**Step 6**: Extract the necessary attributes and put it into the Gold table.

## File Structure

- `ETL_EventHubs_Databricks_Storage.ipynb`: Pipeline for processing streaming data.
- `ETL_Factory_Storage_Databricks.ipynb`: Pipeline for processing batch data.
- `preprocessing.ipynb`: Create data used for pipelines.
- `send.py`: Generate events for Event Hubs and send those to Databricks for process.

## Related Resources
- Dataset: [Kaggle | NYC Yellow Taxi Trip 01-2016](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data?select=yellow_tripdata_2016-01.csv)
- Video Demo:

## References
[Tutorial: Connect to Azure Data Lake Storage Gen2](https://learn.microsoft.com/en-us/azure/databricks/getting-started/connect-to-azure-storage)
