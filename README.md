# Intro
The goal of this project repository is to build a data ingestion application to extract data from multiple sources and leveraging BigQuery as the data warehouse. 

# Prerequisites

1. Docker Engine: <a href="https://docs.docker.com/engine/install/ubuntu/" target="_blank"> Docker Installation Documentation</a>

2. GCP Account (Free trial is offered)
    * Create a BigQuery Project for Metadata Utilities.
    * Create bucket for Metadata project
    * Create a Service Account for Metadata Project
    * Generate a keyfile Metadata Project

3. AWS Account (Free trial is offered) for AWS S3 bucket.

4. Request data sources (Free)
    * Coincap: To Generate API key refer to: <a href="https://docs.coincap.io/#intro" target="_blank">CoinCap - Documentation</a> 
    * Exchange Rate: To Generate API key refer to: <a href="https://www.exchangerate-api.com/" target="_blank"> Exchangerate-api - Documentation</a> 



# Docker Containers

1. **Create Network**

    * ```
      docker network create data-ingestion-network
      ```

2. **Postgres Container**

    * Build Postgres Image
        * ```
          docker build -t data-ingestion-postgres-app-image .
          ```

    * Run Postgres Container with Network
        * ```
          docker run -d --name data-ingestion-postgres-app-container --network data-ingestion-network data-ingestion-postgres-app-image
          ```
    * Run Postgres Container
        * ```
            docker run -d \
            --name data-ingestion-postgres-app-container \
            -e POSTGRES_USER=metadata_user \
            -e POSTGRES_PASSWORD=admin \
            -e POSTGRES_DB=metadata_utilities \
            -p 5432:5432 \
            -v /data:/var/lib/postgresql/data \
            data-ingestion-postgres-app-image
          ```

    * Open an interactive bash session inside the container
        * ```
            docker exec -it metadata-db bash
          ```

    * Inside the container, you can then use psql (PostgreSQL's command-line tool)
        * ```
            psql -U metadata_user metadata_utilities
          ```

3. **Python Container** 

    * Build Python Image
        * ```
            docker build -t data-ingestion-python-app-image .
          ```

    * Run Python Container with Network
        * ```
            docker run -d --name data-ingestion-python-app-container --network data-ingestion-network data-ingestion-python-app-image
          ```

    * Open Interactive bash session inside Python Container
        * ```
            docker exec -it data-ingestion-python-app-container bash
          ```

4. **Spark Container** 

    * Build Spark Image
        * ```
            docker build -t data-ingestion-spark-app-image .
          ```

    * Run Spark Container with Network
        * ```
            docker run -d --name data-ingestion-spark-app-container --network data-ingestion-network data-ingestion-spark-app-image
          ```

    * Open Interactive bash session inside Spark Container
        * ```
            docker exec -it data-ingestion-spark-app-container bash
          ```

# Postgres Metadata Utility Objects

The automation is utilizing a handful of Postgres tables in the `metadata-utilites` database. A user will enter data source connection information, table ingestion 
configurations, and column details into the metadata SQL tables. lastly, the user executes the python controller script with the required parser arguements which 
help identify the source we are accessing. 

The controller script will then pull the data ingestion details from the `metadata-utilities` tables and begin extraction from the data source. Ingesting 
the data into the target GCP storage. As the controller script runs, a log file is generated detailing each action taken, as well as the Start & End datetimess 
of the workflow execution which is recorded in the `workflow_action_history` table with a unique `action_id` for each job execution.



**ER Diagram:**
![alt text](images/metadata_utilities.png)



**INGESTION_CONNECTION_INFO**
This table will hold endpoint connection information as well as encrypted credentials


![alt text](images/connection_info.png)




**INGESTION_CONFIG** 
The Ingestion Config table contains data/table ingestion configurations such as ingestion type, primary key, incremental or full data load, delimiter, file type etc.


![alt text](images/ingestion_config.png)




**INGESTION_COLUMN_DETAILS**
The Ingestion Column Details table contains column details for the data that is being ingested such as column names, data types, ordinal positions, target table


![alt text](images/ingestion_column_details.png)




**WORKFLOW_ACTION_HISTORY**
The Workflow Action History table contains an audit log of all workflow actions including connection_name, Start & End datetimestamps, the Workflow Execution Status and a 
unique `action_id` for each data ingestion workflow that is executed.


| Interpretation | Status |
|----------------|--------|
| In-Progress    | 0      |
| Complete       | 1      |
| Failed         | -1     |



![alt text](images/workflow_action_history.png)



**WORKFLOW_AUDIT_DETAILS**
The Workflow Audit Details table contains statistical metadata for workflow actions such as record counts, deltas, variances and standard deviations.
Each workflow audit record has a unique `audit_id` amd map to the `workflow_action_history` table by `action_id`.


| Audit Details        | Description                                                                                        |
|----------------------|----------------------------------------------------------------------------------------------------|
| execution_duration   | The duration of the workflow execution in minutes.                                                 |
| record_cnt_before    | The record count for the target reference table prior to data load.                                |
| record_cnt_after     | The record count for the target reference table after data load.                                   |
| delta                | Record count delta  (record count after - record count before) for target reference table.         |
| delta_avg            | Record count delta average for the last 7 job executions for target reference table.               |
| delta_variance       | Record count delta variance for the last 7 job executions for target reference table.              |
| delta_std_deviation  | Record count delta standard deviation for the last 6 job executions for target reference table.    |


![alt text](images/workflow_audit_details.png)


# Log Format:

As the script runs a log will be recorded and uploaded to the `workflow_execution_details` bucket path. 
The log file will use the format `YYYYMMDD_<process_id>_<ingestion_type>_<connection_name>_<asset>.log`
Example: `20241215_100124_REQUEST_COINCAP_RATES_RATES.log`



# Data Ingestions: 

**REQUEST**

For a web request data source you will require the following components for a successful communication between the client and server.

* Endpoint URL: Specific address where the API is hosted. 
* Headers: Provides essential information for the server to process the request.
    * Content-Type: Specifies the format of the data being sent.
    * Authorization: Authentication information like API keys, tokens and/or other credentials
* Parameters: These can be included in the URL query string or in the request body. Parameters are used to specify additional details about the request, such as filters, sorting criteria, or pagination.
* Body: For POST, PUT, and PATCH requests, the body contains the data to be sent to the server. This could be in JSON, XML, or other formats
* Authentication: Depending on the API, you may need to provide authentication details like API keys, OAuth tokens, or other credentials.
* Versioning: Some APIs require specifying a version in the URL or headers to ensure compatibility with the correct API version.
* Rate Limiting: Be aware of any rate limits imposed by the API provider to avoid hitting usage limits.


**AWS S3 BUCKET**

AWS SDK for python to support S3 as a data source is available. The requirements to establish a S3 connection are the following.

* AWS Access Key
* AWS Security Token
* Bucket Name
* Prefix Path (Object Name)
* File Name

Boto3: <a href="https://boto3.amazonaws.com/v1/documentation/api/latest/index.html" target="_black">Boto3 - Documentation</a>


**GCS BUCKET**

The Google Cloud Storage SDK for Python, also known as the Google Cloud Storage Python Client, is a library that allows you to interact with Google Cloud Storage from within your Python code.

* key file ()
* Storage Bucket Name
* Prefix Path
* File Name

Google Cloud Storage: <a href="https://cloud.google.com/python/docs/reference/storage/latest" target="_black">Python Client for Google Cloud Storage</a>

**SFTP**

TBD ...



## Supported File Formats: 

* CSV
* TSV
* DAT
* JSON (TBD)
* Parquet (TBD)
* XLS (TBD)
* XLSX (TBD)


## Parser details

**Description: The data will be first loaded to a Flat File**

* **-s:** The section name of configuration file, which will be used to direct workflow execution.

* **-cn:** The connection name of the data source, which will be used to get the data source connection details.

* **-a:** The asset name which will be used to get the object information.

* **-lt:** Overrides the data load type. If not specified, the program will use the value in `ingestion_config` SQL table
    * FULL
    * INCR

* **-c:** The configuration file to be used. If not specified, the program will try to find it with "./config.ini"

* **-l:** Logging level, "info" by default.
    * info
    * debug
    * warning
    * error

* **--print_log:** Whether print the log to console. False by default


## Example Data Ingestion Executions:
**NOTE:** The First data load should be a `FULL` data load. If `ingestion_config` is configured for `Incremental` you can override.

* **REQUEST W/ LOAD TYPE OVERRIDE:** 
```
python3 controller.py -s REQUEST -cn COINCAP -a BITCOIN_HISTORY -lt FULL -l info
```


* **REQUEST W/ CONFIGURATION FILE OVERRIDE:** 
```
python3 controller.py -s REQUEST -cn COINCAP -a ASSETS -c ./new_config.ini -l info
```


* **REQUEST W/ PRINT LOG ON TERMINAL:** 
``` 
python3 controller.py -s REQUEST -cn COINCAP -a ASSETS -l info  --print_log
```


* **AWS S3 BUCKET:** 
```
python3 controller.py -s S3 -cn S3_COINCAP -a SOLANA_HISTORY -l info
```

* **GOOGLE CLOUD STORAGE:**
```
python3 controller.py -s GCS -cn GCS_COINCAP -a SOLANA_HISTORY -l info
```