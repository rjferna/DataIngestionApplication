## Intro
The goal of this project repository is to build a data ingestion application to extract data from multiple sources and leveraging BigQuery as the data warehouse. 

## Prerequisites

1. GCP Account (Free trial is offered)
    * Create a BigQuery Project for Metadata Utilities.
    * Create bucket for Metadata project
    * Create a Service Account for Metadata Project
    * Generate a keyfile Metadata Project

2. Docker Engine: <a href="https://docs.docker.com/engine/install/ubuntu/" target="_blank"> Docker Installation Documentation</a>

3. CoinCap API Key
    * To Generate API key refer to: <a href="https://docs.coincap.io/#intro" target="_blank">CoinCap - Documentation</a> 



## Postgres Metadata Utility Objects

The automation is utilizing a handful of Postgres tables in the `metadata-utilites` database. A user will enter data source connection information, table ingestion 
configurations, and column details into the SQL tables. lastly, the user executes the python controller script with the required data ingestion arguements which 
help identify the source we are accessing for extraction. 

The controller script will then pull the required data ingestion details from the `metadata-utilities` tables and begin extraction from the data source. Writing 
the data into the target GCP storage. As the controller script runs, a log file is generated detailing each action taken by the controller script. As well as the 
Start & End datetimestamps of the workflow execution which is recorded in the `workflow_action_history` table with a unique `action_id` for each job execution.



**ER Diagram:**
![alt text](git/DataIngestionApplication/images/metadata_utilities.png)



**INGESTION_CONNECTION_INFO**
This table will hold endpoint connection information as well as encrypted credentials


![alt text](git/DataIngestionApplication/images/connection_info.png)




**INGESTION_CONFIG** 
The Ingestion Config table contains data/table ingestion configurations such as ingestion type, primary key, incremental or full data load, delimiter, file type etc.


![alt text](git/DataIngestionApplication/images/ingestion_config.png)



**INGESTION_COLUMN_DETAILS**
The Ingestion Column Details table contains column details for the data that is being ingested such as column names, data types, ordinal positions, target table


![alt text](git/DataIngestionApplication/images/ingestion_column_details.png)



**WORKFLOW_ACTION_HISTORY**
The Workflow Action History table contains an audit log of all workflow actions including connection_name, Start & End datetimestamps, the Workflow Execution Status and a 
unique `action_id` for each data ingestion workflow that is executed.


| Interpretation | Status |
|----------------|--------|
| In-Progress    | 0      |
| Complete       | 1      |
| Failed         | -1     |



![alt text](git/DataIngestionApplication/images/workflow_action_history.png)


**WORKFLOW_AUDIT_DETAILS**
The Workflow Audit Details table contains statistical metadata for workflow actions such as record counts, deltas, variances and standard deviations.
Each workflow audit record has a unique `audit_id` amd map to the `workflow_action_history` table by `action_id`.



**CHANGE_EVENT**

TBD...


## Log Format:

As the script runs a log will be recorded and uploaded to the `workflow_execution_details` bucket path. 
The log file will use the format `YYYYMMDD_<process_id>_<ingestion_type>_<connection_name>_<asset>.log`
Example: `20241215_100124_REQUEST_COINCAP_RATES_RATES.log`



## Data Ingestions: 

**CoinCap**

CoinCap is a useful tool for real-time pricing and market activity for over 1,000 cryptocurrencies. By collecting exchange data from thousands of markets, we are able to offer
transparent and accurate data on asset price and availability. 


CoinCap: <a href="https://docs.coincap.io/#intro" target="_blank">CoinCap - Documentation</a>



**AWS S3 BUCKET**

AWS SDK for python to support S3 as a data source is available. The requirements to establish a S3 connection are the following.
* AWS Access Key
* AWS Security Token
* Bucket Name
* Prefix Path (Object Name)
* File Name

Boto3: <a href="https://boto3.amazonaws.com/v1/documentation/api/latest/index.html" target="_black">Boto3 - Documentation</a>


**GCS BUCKET**

TBD ...



**SFTP**

TBD ...


## File Formats: 

* CSV
* JSON (TBD)
* DAT
* XLS
* XLSX


### Parser details

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


### Example Data Ingestion Executions:
**NOTE:** The First data load should always be a `FULL` data load. If `ingestion_config` is configured for `Incremental` you can override.

* **CRYPTO BITCOIN_HISTORY W/ LOAD TYPE OVERRIDE:** 
```
python3 controller.py -s REQUEST -cn COINCAP_BITCOIN_HISTORY -a BITCOIN_HISTORY -lt FULL -l info --print_log
```


* **CRYPTO ASSET W/ PRESET CONFIG:** 
```
python3 controller.py -s REQUEST -cn COINCAP_ASSETS -a ASSETS -c ./new_config.ini -l info
```


* **Crypto Asset data:** 
``` 
python3 controller.py -s REQUEST -cn COINCAP_ASSET -a ASSETS -l info 
```


* **CRYPTO EXCHANGE DATA:** 
```
python3 controller.py -s REQUEST -cn COINCAP_EXCHANGES -a EXCHANGES -l info
```


* **CRYPTO COIN RATES:** 
```
python3 controller.py -s REQUEST -cn COINCAP_RATES -a RATES -l info
```


* **CRYPTO MARKETS DATA:** 
```
python3 controller.py -s REQUEST -cn COINCAP_MARKETS -a MARKETS -l info
```


* **S3 (DUMMY DATA):** 
```
python3 controller.py -s S3 -cn S3_COINCAP -a SOLANA_HISTORY -l info

