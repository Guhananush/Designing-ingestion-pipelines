# Designing-ingestion-pipelines

Designing an ingestion pipeline means planning how data is collected from different source systems and moved into a data platform (such as a data lake, lakehouse, or data warehouse) in a reliable, scalable, secure, and efficient way.

## Basics
Before we talk about design process, first some basic concepts. Might be obvious for many of us, but still worth repeating in case you are new to data ingestion.

## ETL vs ELT vs ETLT
This is pretty much a common knowlege, so I will not spend much time on this topic. Depending on your prefered toolset and the destination data format, we have 3 ways to move the data from external system into our data store.

<img width="851" height="382" alt="image" src="https://github.com/user-attachments/assets/9d4d5a22-7f84-42ab-97a4-36fa7a3aada9" />

ETLs are simple. You get the RAW data from external source. You save it (as RAW) into some object storage system (GCS bucket, AWS S3 etc.). Then you run some process to transform this data into a form digestable by your destination and load it into the final store, which in most cases is a Data Warehouse. I like it for its simplicity as my tool of choice for both extraction and transformation is Python.                     
One of the things I make sure I add during the transformation process are technical timestamps (more on this later). Because Data Warehouse usually requires data to fit certain schema, we need to make sure that the transformed data has all the values in correct format. So other transformations might be data format changes, numeric values changes (converting commas to dots) etc. It’s mostly just basic cleanup.         

<img width="835" height="276" alt="image" src="https://github.com/user-attachments/assets/bbeda14c-ae64-4362-b61e-380b8317bad6" />

E(T)LT pipelines are used whenever you can just drop the data safely into some DWH storage, f.e. Data Lake where the format of your data doesn’t really matter. You could probably just drop the RAW data there, but I would still prefer adding some technical timestamps like when the data was extracted from source etc.              
Once you have it in your DWH staging area, you can use SQL or maybe Spark code to clean it up using DWH/Data Lake resource. So you extracted data, loaded it and now you are transforming. Cleaned and well formatted data can be now added to a DWH destination table and used by Analysts or Data Scientists.

Which patterns is better? It might depend on your preferred toolset (language) and data volume (maybe Python is not enough to clean up 1 million records?). In most cases, when we are not talking about Big Data, ETL is enough. Even if it’s few hundred thousends of records per download, your Python cleanup code can finish the transformations in less then a minute. And this pattern seem to be fairly easy to use.          
If you prefer cleaning data with Spark or SQL, or volumes are big enough to require some serious processing power, ELT is more useful for you.

## Technical timestamps
I mentioned it few time, so to clarify: what are technical timestamps. Those are timestamps you add to each and every record whenever you process the data. At my current company, we are using Airflow to orchestrate the ETL pipelines, and this process adds 3 timestamps to each record:




