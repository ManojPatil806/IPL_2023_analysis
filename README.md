# IPL_2023_analysis
<h3>Project Overview:</h3>
This project aims to provide a data analysis solution for IPL 2023 using Azure Databricks and Azure Data Factory and Power BI. This is an ETL pipeline to ingest IPL 2023 data, transform and load it into our data warehouse for reporting and analysis purposes. The data is sourced from github repo and is stored in Azure Datalake Gen2 storage. Data transformation and analysis were performed using Azure Databricks and Power BI. The entire process is orchestrated using Azure Data Factory.

<h3>Architecture diagram</h3>

<img src="https://github.com/ManojPatil806/IPL_2023_analysis/blob/main/Screenshots/arche.png">

# ER Diagram:

The structure of the database is shown in the following ER Diagram and explained in the [Database User Guide](http://ergast.com/docs/f1db_user_guide.txt)
![ERDiagram](http://ergast.com/images/ergast_db.png)

## How it works:
<h3>Source Date Files</h3>
We are referring to open-source data from the github repository. Data was available from 2023.

| File Name  | File Type |
| ------------- | ------------- |
| Batsman  | CSV  |
| Bowler | CSV |
| Match_Scoreboard  | CSV  |
| Matches | CSV |


#### Execution Overview:
- Azure Data Factory (ADF) is responsible for the execution of Azure Datarbicks notebooks as well as monitoring them. We import data from github repository to Azure Data Lake Storage Gen2 (ADLS). The raw data is stored in the container at raw-ipl-data (landing zone).
- The transform data in Azure Databricks load into the Azure SQL DB using JDBC connection 
- Make the connection in Power BI with Azure Sql DB to generate report.


## Azure Resources Required for this Project:
* Azure Data Lake Storage
* Azure Data Factory
* Azure Databricks
* Azure Sql DB


## Create Azure Data lake Storage:
<img src="https://github.com/ManojPatil806/IPL_2023_analysis/blob/main/Screenshots/storage%20gen2.png">

## Create Azure Factory:
<img src="https://github.com/ManojPatil806/IPL_2023_analysis/blob/main/Screenshots/ADF.png">

## Create Azure Databricks:
<img src="https://github.com/ManojPatil806/IPL_2023_analysis/blob/main/Screenshots/ADB.png">

## Create Azure Sql DB:
<img src="https://github.com/ManojPatil806/IPL_2023_analysis/blob/main/Screenshots/az-db.png">

#### Step1. Data Ingestion 
* Ingest all 4 files in CSV formate into Azure data lake using Azure data factory and create contaniner name
as **raw-ipl-data**. 
<img src="https://github.com/ManojPatil806/IPL_2023_analysis/blob/main/Screenshots/Screenshot%202023-12-12%20194045.png">

<img src="https://github.com/ManojPatil806/IPL_2023_analysis/blob/main/Screenshots/Screenshot%202023-12-12%20194224.png">

#### Step2. Make the connection with ADLS gen2 in Azure Databricks for data transformation.
[Source code](https://github.com/ManojPatil806/IPL_2023_analysis/blob/main/code/MATCH_ANALYSIS_TBL.ipynb)
* Create Data frame to do the transformation.
* Join table using SQL query.
* Load the data usning JDBC connection in target location.

  
#### 3. Data Reporting Requirements
* We want to be able to know Driver Standings.
* We should be able to know Constructor Standings as well.

#### 4. Data Analysis Requirements
* Find the Dominant drivers.
* Find the Dominant Teams. 
* Visualize the Outputs.
* Create Databricks dashboards.

#### 5. Scheduling Requirements
* Scheduled to run every Sunday at 10 pm.
* Ability to monitor pipelines.
* Ability to rerun failed pipelines.
* Ability to set up alerts on failures

#### 6. Other Non-Functional Requirements
* Ability to delete individual records
* Ability to see history and time travel
* Ability to roll back to a previous version

## Analysis Result:
![image](https://user-images.githubusercontent.com/64007718/235310453-95b6d253-aaab-454b-87f1-8fb722600014.png)
![image](https://user-images.githubusercontent.com/64007718/235310459-c9141816-2832-4be7-8902-3fce7096c88d.png)
![image](https://user-images.githubusercontent.com/64007718/235310466-4a83e4ce-00c3-444c-b22a-83ad42530321.png)
![image](https://user-images.githubusercontent.com/64007718/235310470-9c966e29-ba76-4c10-9554-f201d72ee636.png)
![image](https://user-images.githubusercontent.com/64007718/235310476-98db1649-0fb4-45f5-bfc4-8892afc8bc80.png)
![image](https://user-images.githubusercontent.com/64007718/235310486-98404d97-ed11-4be2-90c3-535f538cfdc9.png)

## Tasks performed:
•	Built a solution architecture for a data engineering solution using Azure Databricks, Azure Data Lake Gen2, Azure Data Factory, and Power BI.

•	Created and used Azure Databricks service and the architecture of Databricks within Azure.

•	Worked with Databricks notebooks and used Databricks utilities, magic commands, etc.

•	Passed parameters between notebooks as well as created notebook workflows.

•	Created, configured, and monitored Databricks clusters, cluster pools, and jobs.

•	Mounted Azure Storage in Databricks using secrets stored in Azure Key Vault.

•	Worked with Databricks Tables, Databricks File System (DBFS), etc.

•	Used Delta Lake to implement a solution using Lakehouse architecture.

•	Created dashboards to visualize the outputs.

•	Connected to the Azure Databricks tables from PowerBI.

## Spark (Only PySpark and SQL)
•	Spark architecture, Data Sources API, and Dataframe API.

•	PySpark - Ingested CSV, simple, and complex JSON files into the data lake as parquet files/ tables.

•	PySpark - Transformations such as Filter, Join, Simple Aggregations, GroupBy, Window functions etc.

•	PySpark - Created global and temporary views.

•	Spark SQL - Created databases, tables, and views.

•	Spark SQL - Transformations such as Filter, Join, Simple Aggregations, GroupBy, Window functions etc.

•	Spark SQL - Created local and temporary views.

•	Implemented full refresh and incremental load patterns using partitions.

## Delta Lake
•	Performed Read, Write, Update, Delete, and Merge to delta lake using both PySpark as well as SQL.

•	History, Time Travel, and Vacuum.

•	Converted Parquet files to Delta files.

•	Implemented incremental load pattern using delta lake.

## Azure Data Factory
•	Created pipelines to execute Databricks notebooks.

•	Designed robust pipelines to deal with unexpected scenarios such as missing files.

•	Created dependencies between activities as well as pipelines.

•	Scheduled the pipelines using data factory triggers to execute at regular intervals.

•	Monitored the triggers/ pipelines to check for errors/ outputs.

# About the Project:

<h3>Folders:</h3>

- 1-Authentication: The folder contains all notebooks to demonstrate different ways to access Azure Data Lake Gen2 containers into the Databricks file system.

- 2-includes: The folder contains notebooks with common functions and path configurations.

- 3-Data Ingestion: The folder contains all notebooks to ingest the data from raw to processed.

- 4-raw: The folder contains all notebooks to create raw tables in SQL.

- 5-Data Transformation: The folder contains all notebooks that transform the raw data into the processed layer.

- 6-Data Analysis: The folder contains all notebooks which include an analysis of the data.

- 7-demo: The folder contains notebooks with all the pre-requisite demos.

- 8-Power Bi reports: This folder contains all the reports created from the analyzed data.

<h3>Technologies/Tools Used:</h3>
<ul>
  <li>Pyspark</li> 
  <li>Spark SQL</li> 
  <li>Delta Lake</li> 
  <li>Azure Databricks </li> 
  <li>Azure Data Factory</li> 
  <li>Azure Date Lake Storage Gen2</li> 
  <li>Azure Key Fault</li> 
  <li>Power BI</li> 
</ul>  
