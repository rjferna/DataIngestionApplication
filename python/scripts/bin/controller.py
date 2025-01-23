import os
import sys
import json
import shutil
import time
from datetime import datetime, timezone, timedelta

from config import Config
from parse_args import parse_args
from logger import set_logger
from aws_common import get_aws_s3
from postgres_common import PostgresDB
from postgres_query import (
    get_connection_details,
    get_column_details,
    set_workflow_action_history_record,
    get_workflow_action_id,
    update_workflow_action_history_record,
    set_workflow_audit_details_record,
    update_workflow_audit_details_record,
)
from gcp_common import (
    archive_file,
    get_gcp_storage,
    upload_to_bucket,
    gcp_execute_query,
)
from gcp_query import (
    get_incremental_date,
    get_record_count,
    get_table_exists,
    create_external_table,
    create_and_load_staging_table,
    create_and_load_reference_table,
)
from requests_common import get_request

from encryption_decryption_common import Prpcrypt
from file_common import response_to_parquet, csv_to_parquet


def main():
    args = parse_args(
        "Data Ingestion Controller", "Controller script for data ingestion modules."
    )

    # Read the Configuration file
    config_parser = Config(args.get("config_file"))
    config_var = config_parser.get(args["section"])

    # Logging level setup
    log_domain = "{}_{}_{}".format(
        args["section"], args["connection"], args.get("asset")
    )
    logger = set_logger(
        args.get("log_level"),
        log_domain,
        print_log=args["print_log"],
        log_path=config_var.get("log_path"),
    )
    logger.debug("args: {}".format(args))

    try:
        # Initialize Execution Start Datetime (UTC)
        execution_start_datetime_utc = datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S.%f"
        )

        # Makes data directory if not exists
        if os.path.exists(config_var.get("file_path")):
            if not os.listdir(config_var.get("file_path")):
                logger.info(f"No files in directory: {config_var.get('file_path')}")
            elif len(os.listdir(config_var.get("file_path"))) >= 10:
                shutil.make_archive(
                    config_var.get("archive_path")
                    + str(datetime.now().strftime("%Y%m%d%H%M%S")),
                    "zip",
                    config_var.get("file_path"),
                )
                logger.info(
                    f"Archived files in directory: {config_var.get('file_path')}"
                )
                shutil.rmtree(config_var.get("file_path"))
        os.makedirs(config_var.get("file_path"), exist_ok=True)

        # Get Connection Details from GCP
        logger.info(
            f"Querying Postgres for Data Ingestion Details: ({args.get('section')}, {args.get('connection')} ,{args.get('asset')})"
        )

        db = PostgresDB(
            host=config_var.get("host"),
            database=config_var.get("database"),
            user=config_var.get("user"),
            password=config_var.get("password"),
        )

        db.open_connection()

        logger.info("Postgres Connection Open...")

        query = get_connection_details(args.get("connection"), args.get("asset"))

        results = db.execute_query(query)

        # Assign Data Ingestion Details
        connection_name = results.get("connection_name")
        connection_url = results.get("connection_url")
        user_name = results.get("user_name")
        password_encrypted = results.get("password_encrypted")
        security_token = json.loads(results.get("security_token"))
        ingestion_type = results.get("ingestion_type")
        source_schema_table_name = results.get("source_schema_table_name")
        project_id = results.get("project_id")
        database_schema = results.get("database_schema")
        table_name = results.get("table_name")
        primary_key_column = results.get("primary_key_column")
        incremental_date_column = results.get("incremental_date_column")
        extra_parameters = results.get("extra_parameters")
        file_format = results.get("file_format")
        header = results.get("header")
        delimiter = results.get("delimiter")
        quote_characters = results.get("quote_characters")
        escape_characters = results.get("escape_characters")
        is_parquet = results.get("is_parquet")
        to_parquet = results.get("to_parquet")
        accepted_encoding = results.get("accepted_encoding")
        bucket = results.get("bucket")
        bucket_destination = results.get("bucket_destination")
        archive_destination = results.get("archive_destination")
        if args.get("load_type_override") == None:
            load_type = results.get("load_type")
        else:
            load_type = args.get("load_type_override")

        # Set Workflow Action History Job Execution Record
        logger.info("Set Workflow Action History Execution Record")
        query = set_workflow_action_history_record(
            connection_name=connection_name,
            database_schema=database_schema,
            table_name=table_name,
            user_name=user_name,
            execution_start_datetime=execution_start_datetime_utc,
            execution_status=0,
        )

        results = db.execute_query(query)

        # Sleep for 3 seconds
        time.sleep(3)

        # Get Process_Id from GCP workflow_action_history
        logger.info(f"Get Worflow Action History Action_Id.")

        query = get_workflow_action_id()

        results = db.execute_query(query)

        action_id = results.get("action_id")

        logger.info(f"Workflow Action ID: {action_id}")

        # Set logfilepath and logfile
        logfilepath = config_var.get("log_path") + "{:%Y%m%d}_{}.log".format(
            datetime.now(), log_domain
        )

        logfile = config_var.get(
            "log_bucket_workflow_execution_destination"
        ) + "{:%Y%m%d}_{}_{}.log".format(datetime.now(), action_id, log_domain)

        # Confirm Section
        if ingestion_type != args.get("section"):
            logger.info(
                f"[Error]: Section type does not match ingestion configuration."
            )

        logger.info(f"Beginning {args.get('section')} Data Ingestion")

        # Identify Data Ingestion Workflow type
        if ingestion_type == "REQUEST":
            # Decrypt Credentials
            logger.info("Obtaining Connection Credentials")
            pc = Prpcrypt(security_token["access"])

            if incremental_date_column == None:
                response = get_request(
                    key=pc.decrypt(password_encrypted),
                    url=connection_url,
                    encoding=accepted_encoding,
                    incremental_start_date=None,
                    incremental_end_date=None,
                    interval=None,
                )

            elif incremental_date_column is not None:
                # Check if Reference table Exists, if not set default incremental start date
                logger.info(
                    f"Checking if Reference Table Exists: {project_id}.REF_{database_schema}.{table_name}"
                )

                query = get_table_exists(
                    project_id=project_id,
                    dataset=database_schema,
                    table_name=table_name,
                )

                ref_exists = gcp_execute_query(
                    query=query,
                    return_response=1,
                    keyfile_path=config_var.get("gcp_creds"),
                )

                if (
                    str(ref_exists) == str
                    or ref_exists == 0
                    or (ref_exists == 1 and load_type == "FULL")
                ):
                    incr_result = (datetime.now() - timedelta(days=3)).strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    )
                    logger.info(f"Incremental Start Datetime: {incr_result}")

                    response = get_request(
                        key=pc.decrypt(password_encrypted),
                        url=connection_url,
                        encoding=accepted_encoding,
                        incremental_start_date=incr_result,
                        incremental_end_date=datetime.now().strftime(
                            "%Y-%m-%dT%H:%M:%SZ"
                        ),
                        interval=extra_parameters,
                    )
                else:
                    logger.info("Get last incremental loadtime")

                    query = get_incremental_date(
                        date=incremental_date_column,
                        project_id=project_id,
                        dataset=database_schema,
                        table_name=table_name,
                    )

                    incr_result = gcp_execute_query(
                        query=query,
                        return_response=1,
                        keyfile_path=config_var.get("gcp_creds"),
                    )

                    logger.info(f"Data collection start datetime: {incr_result}")

                    response = get_request(
                        key=pc.decrypt(password_encrypted),
                        url=connection_url,
                        encoding=accepted_encoding,
                        incremental_start_date=incr_result,
                        incremental_end_date=datetime.now().strftime(
                            "%Y-%m-%dT%H:%M:%SZ"
                        ),
                        interval=extra_parameters,
                    )
        elif ingestion_type == "S3":
            # Decrypt Credentials
            logger.info("Obtaining Connection Credentials")
            pc = Prpcrypt(security_token["access"])

            response = get_aws_s3(
                aws_access_key=pc.decrypt(password_encrypted),
                aws_security_token=security_token["token"],
                bucket_path=connection_url,
                prefix_path=source_schema_table_name,
                import_path=config_var.get("file_path"),
                import_file="{}.{}".format(table_name, file_format),
            )
        elif ingestion_type == "GCS":
            # Decrypt Credentials
            logger.info("Obtaining Connection Credentials")
            pc = Prpcrypt(security_token["token"])

            decrypted_credential = json.loads(pc.decrypt(password_encrypted))

            pc = Prpcrypt(security_token["access"])

            gcs_creds = {}
            for name, val in decrypted_credential.items():
                gcs_creds.update({name: pc.decrypt(val)})

            query = get_gcp_storage(
                bucket_name=connection_url,
                prefix_path=source_schema_table_name,
                import_path=config_var.get("file_path"),
                import_file="{}.{}".format(table_name, file_format),
            )

            response = gcp_execute_query(
                query=query, return_response=1, keyfile=config_var.get("gcp_creds")
            )

        # Sleep for 3 seconds
        time.sleep(3)

        # Check Response Data & Write to Parquet
        if "Error:" in response:
            logger.info(f"{response}")

            query = update_workflow_action_history_record(
                action_id=action_id, execution_status=-1
            )

            results = db.execute_query(query)

            logger.info(f"Error: Updating Workflow Action Record: {results}")
        else:
            if ingestion_type == "REQUEST":
                logger.info(f"Response Type: {type(response)}")
                response_file = response_to_parquet(
                    response_data=response,
                    parquet_filename=config_var.get("file_path") + table_name,
                    compression=accepted_encoding,
                )
                logger.info(f"Writing response data to file")
            elif ingestion_type in ("S3", "GCS", "SFTP"):
                if to_parquet == True:
                    if file_format == "CSV":
                        logger.info(f"Converting {file_format} to PARQUET.")
                        response_file = csv_to_parquet(
                            file_path=response,
                            header=header,
                            seperator=delimiter,
                            quotation=quote_characters,
                            parquet_filename=config_var.get("file_path") + table_name,
                            compression=accepted_encoding,
                        )
                    elif file_format == "TSV":
                        logger.info(f"Converting {file_format} to PARQUET.")
                        # response_file = tsv_to_parquet()
                    elif file_format == "JSON":
                        logger.info(f"Converting {file_format} to PARQUET.")
                        # response_file = json_to_parquet()
                    elif file_format == "AVRO":
                        logger.info(f"Converting {file_format} to PARQUET.")
                        # response_file = avro_to_parquet()
                    file_format = "PARQUET"
                    logger.info(f"File Conversion Completed.")
                else:
                    logger.info(f"File remaining as: {file_format}")

        # Sleep for 3 seconds
        time.sleep(3)

        # Check File Conversion
        if "Error:" in response_file:
            logger.info(f"{response_file}")

            query = update_workflow_action_history_record(
                action_id=action_id, execution_status=-1
            )

            results = db.execute_query(query)

            logger.info(f"Error: Updating Workflow Action Record: {results}")

            logger.info(
                "Data Ingestion Completed with Errors." + "\n" + "Execution END."
            )

            # Upload Workflow Execution Log File to GCP Bucket
            upload_to_bucket(
                bucket_name=config_var.get("log_bucket"),
                source_file_name=logfilepath,
                destination_blob_name=logfile,
                keyfile_path=config_var.get("gcp_creds"),
            )

            db.close_connection()
            sys.exit(1)

        # Sleep for 3 seconds
        time.sleep(3)

        # Archive Existing File in GCP Bucket
        logger.info(
            f"Checking to see if file exists: {bucket_destination + table_name + '.' + file_format}"
        )

        archive_response = archive_file(
            source_bucket_name=bucket,
            source_file_name=bucket_destination
            + table_name
            + "."
            + file_format.lower(),
            archive_bucket_name=bucket,
            archive_destination=archive_destination,
            archive_file_name=table_name + "." + file_format.lower(),
            keyfile_path=config_var.get("gcp_creds"),
        )
        if "Error:" in archive_response:
            logger.info(f"Error: {archive_response}")
        else:
            logger.info(f"Archive Status: {archive_response}")

        # Sleep for 3 seconds
        time.sleep(3)

        # Load New Data File To GCP Bucket
        logger.info(
            f"Uploading Data to Bucket Path: {bucket_destination}{table_name}.{file_format}"
        )
        upload_data = upload_to_bucket(
            bucket_name=bucket,
            source_file_name=response_file,
            destination_blob_name=bucket_destination
            + table_name
            + "."
            + file_format.lower(),
            keyfile_path=config_var.get("gcp_creds"),
        )
        if "Error:" in upload_data:
            logger.info(f"{upload_data}")

            query = update_workflow_action_history_record(
                action_id=action_id, execution_status=-1
            )

            results = db.execute_query(query)

            logger.info(f"Error: Updating Workflow Action Record: {results}")

            logger.info(
                "Data Ingestion Completed with Errors." + "\n" + "Execution END."
            )

            # Upload Workflow Execution Log File to GCP Bucket
            upload_to_bucket(
                bucket_name=config_var.get("log_bucket"),
                source_file_name=logfilepath,
                destination_blob_name=logfile,
                keyfile_path=config_var.get("gcp_creds"),
            )

            db.close_connection()
            sys.exit(1)

        # Sleep for 3 seconds
        time.sleep(3)

        # Create External Table
        logger.info(
            f"Creating External Table: {project_id}.EXTERNAL_{database_schema}.{args.get('asset')}"
        )

        query = create_external_table(
            project_id=project_id,
            dataset=database_schema,
            table_name=table_name,
            bucket_destination_name=bucket
            + "/"
            + bucket_destination
            + table_name
            + "."
            + file_format.lower(),
            file_format=file_format,
        )

        create_external = gcp_execute_query(
            query=query, return_response=0, keyfile_path=config_var.get("gcp_creds")
        )

        if "Error:" in create_external:
            logger.info(f"{create_external}")

            query = update_workflow_action_history_record(
                action_id=action_id, execution_status=-1
            )

            results = db.execute_query(query)

            logger.info(f"Error: Updating Workflow Action Record: {results}")

            logger.info(
                "Data Ingestion Completed with Errors." + "\n" + "Execution END."
            )

            # Upload Workflow Execution Log File to GCP Bucket
            upload_to_bucket(
                bucket_name=config_var.get("log_bucket"),
                source_file_name=logfilepath,
                destination_blob_name=logfile,
                keyfile_path=config_var.get("gcp_creds"),
            )

            db.close_connection()
            sys.exit(1)

        # Get Column Details from GCP
        logger.info(f"Querying database for Column Details.")

        query = get_column_details(
            project_id=project_id,
            database_schema=database_schema,
            table_name=table_name,
        )

        results = db.execute_query(query)

        if "Error:" in query:
            logger.info(f"{query}")

            update_workflow_action_history_record(
                action_id=action_id, execution_status=-1
            )

            results = db.execute_query(query)

            logger.info(f"Error: Updating Workflow Action Record: {results}")

            logger.info(
                "Data Ingestion Completed with Errors." + "\n" + "Execution END."
            )

            # Upload Workflow Execution Log File to GCP Bucket
            upload_to_bucket(
                bucket_name=config_var.get("log_bucket"),
                source_file_name=logfilepath,
                destination_blob_name=logfile,
                keyfile_path=config_var.get("gcp_creds"),
            )

            db.close_connection()
            sys.exit(1)

        # Assign Data Ingestion Details
        stg_and_ref_create_table = results.get("stg_ref_create_table_column_details")
        source_to_stg_conversion = results.get(
            "source_to_stg_conversion_column_details"
        )
        source_to_stg_column_query = results.get("source_to_stg_column_query")
        mapping_stg_to_ref_column_query = results.get("mapping_stg_to_ref_column_query")

        # Sleep for 3 seconds
        time.sleep(3)

        # Drop & Create Staging Table
        logger.info(
            f"Creating and loading Staging Table: {project_id}.STG_{database_schema}.{table_name}"
        )
        query = create_and_load_staging_table(
            project_id=project_id,
            dataset=database_schema,
            table_name=table_name,
            stg_and_ref_create_table=stg_and_ref_create_table,
            source_to_stg_conversion=source_to_stg_conversion,
        )

        create_load_staging = gcp_execute_query(
            query=query, return_response=0, keyfile_path=config_var.get("gcp_creds")
        )

        if "Error:" in create_load_staging:
            logger.info(f"{create_load_staging}")

            query = update_workflow_action_history_record(
                action_id=action_id, execution_status=-1
            )

            results = db.execute_query(query)

            logger.info(f"Error: Updating Workflow Action Record: {results}")

            logger.info(
                "Data Ingestion Completed with Errors." + "\n" + "Execution END."
            )

            # Upload Workflow Execution Log File to GCP Bucket
            upload_to_bucket(
                bucket_name=config_var.get("log_bucket"),
                source_file_name=logfilepath,
                destination_blob_name=logfile,
                keyfile_path=config_var.get("gcp_creds"),
            )

            db.close_connection()
            sys.exit(1)

        # Sleep for 3 seconds
        time.sleep(3)

        # Check if Reference table Exists if not Create Reference table
        logger.info(
            f"Checking if Reference Table Exists: {project_id}.REF_{database_schema}.{table_name}"
        )
        query = get_table_exists(
            project_id=project_id, dataset=database_schema, table_name=table_name
        )

        ref_exists = gcp_execute_query(
            query=query, return_response=1, keyfile_path=config_var.get("gcp_creds")
        )

        if type(ref_exists) == str:
            logger.info(f"{ref_exists}")

            query = update_workflow_action_history_record(
                action_id=action_id, execution_status=-1
            )

            results = db.execute_query(query)

            logger.info(f"Error: Updating Workflow Action Record: {results}")

            logger.info(
                "Data Ingestion Completed with Errors." + "\n" + "Execution END."
            )
        elif ref_exists == 0:
            logger.info(
                f"Reference Table Flag: {ref_exists}. Table does not exists, Creating table & loading in staging data."
            )
            query = create_and_load_reference_table(
                flag=0,
                project_id=project_id,
                dataset=database_schema,
                table_name=table_name,
                stg_and_ref_create_table=stg_and_ref_create_table,
                mapping_stg_to_ref_query=mapping_stg_to_ref_column_query,
                primary_key_column=primary_key_column,
            )

            create_ref = gcp_execute_query(
                query=query, return_response=0, keyfile_path=config_var.get("gcp_creds")
            )

            logger.info(f"Get Reference Table Record Count.")

            query = get_record_count(
                project_id=project_id, dataset=database_schema, table_name=table_name
            )

            record_cnt = gcp_execute_query(
                query=query, return_response=1, keyfile_path=config_var.get("gcp_creds")
            )

            logger.info(f"Set Workflow audit details.")

            # Set Workflow Audit Details for Ref Table
            query = set_workflow_audit_details_record(
                action_id=action_id,
                connection_name=connection_name,
                database_schema=database_schema,
                table_name=table_name,
                execution_start_datetime=execution_start_datetime_utc,
                record_count=record_cnt,
            )

            results = db.execute_query(query)

            logger.info(f"Workflow Audit Details: {results}")

            logger.info(
                f"Drop & Create Reference table: {create_ref}. Full dataload Completed."
            )
        elif ref_exists == 1 and load_type == "FULL":
            logger.info(
                f"Reference Table Flag: {ref_exists} Table exists. Data load type: {load_type}. Drop & Creating table and full refresh."
            )

            logger.info(f"Get Reference Table Record Count.")

            query = get_record_count(
                project_id=project_id, dataset=database_schema, table_name=table_name
            )

            record_cnt = gcp_execute_query(
                query=query, return_response=1, keyfile_path=config_var.get("gcp_creds")
            )

            logger.info(f"Set Workflow audit details.")

            # Set Workflow Audit Details for Ref Table
            query = set_workflow_audit_details_record(
                action_id=action_id,
                connection_name=connection_name,
                database_schema=database_schema,
                table_name=table_name,
                execution_start_datetime=execution_start_datetime_utc,
                record_count=record_cnt,
            )

            results = db.execute_query(query)

            logger.info(f"Workflow Audit Details: {results}")

            query = create_and_load_reference_table(
                flag=0,
                project_id=project_id,
                dataset=database_schema,
                table_name=table_name,
                stg_and_ref_create_table=stg_and_ref_create_table,
                mapping_stg_to_ref_query=mapping_stg_to_ref_column_query,
                primary_key_column=primary_key_column,
            )

            create_ref = gcp_execute_query(
                query=query, return_response=0, keyfile_path=config_var.get("gcp_creds")
            )

            logger.info(
                f"Drop & Create Reference table: {create_ref}. Full dataload Completed."
            )
        elif ref_exists == 1 and load_type == "INCR":
            logger.info(
                f"Reference Table Flag: {ref_exists} Table exists. Data load type: {load_type}. Begin Incremental Data Load."
            )

            logger.info(f"Get Reference Table Record Count.")

            query = get_record_count(
                project_id=project_id, dataset=database_schema, table_name=table_name
            )

            record_cnt = gcp_execute_query(
                query=query, return_response=1, keyfile_path=config_var.get("gcp_creds")
            )

            logger.info(f"Set Workflow audit details.")

            # Set Workflow Audit Details for Ref Table
            query = set_workflow_audit_details_record(
                action_id=action_id,
                connection_name=connection_name,
                database_schema=database_schema,
                table_name=table_name,
                execution_start_datetime=execution_start_datetime_utc,
                record_count=record_cnt,
            )

            results = db.execute_query(query)

            logger.info(f"Workflow Audit Details: {results}")

            query = create_and_load_reference_table(
                flag=1,
                project_id=project_id,
                dataset=database_schema,
                table_name=table_name,
                stg_and_ref_create_table=stg_and_ref_create_table,
                mapping_stg_to_ref_query=mapping_stg_to_ref_column_query,
                primary_key_column=primary_key_column,
            )

            create_ref = gcp_execute_query(
                query=query, return_response=0, keyfile_path=config_var.get("gcp_creds")
            )

            logger.info(
                f"Incremental Data Load to Reference table: {create_ref} Completed."
            )

        # Sleep for 3 seconds
        time.sleep(3)

        if "Error:" in create_ref:
            logger.info(f"{create_ref}")

            query = update_workflow_action_history_record(
                action_id=action_id, execution_status=-1
            )

            results = db.execute_query(query)

            logger.info(f"Error: Updating Workflow Action Record: {results}")

            logger.info(
                "Data Ingestion Completed with Errors." + "\n" + "Execution END."
            )

        # Update Workflow Action History Record
        query = update_workflow_action_history_record(
            action_id=action_id, execution_status=1
        )

        results = db.execute_query(query)

        logger.info(f"Updating Workflow Action Record: {results}")

        # Sleep for 1 seconds
        time.sleep(1)

        query = get_record_count(
            project_id=project_id, dataset=database_schema, table_name=table_name
        )

        record_cnt = gcp_execute_query(
            query=query, return_response=1, keyfile_path=config_var.get("gcp_creds")
        )

        # Update Workflow Audit Details to Include Execution Stats
        query = update_workflow_audit_details_record(
            action_id=action_id, table_name=table_name, record_count=record_cnt
        )

        results = db.execute_query(query)

        logger.info(f"Updating Workflow Audit Details: {results}")

        logger.info(f"Execution END.")

        # Sleep for 1 seconds
        time.sleep(1)

        # Upload Workflow Execution Log File to GCP Bucket
        upload_to_bucket(
            bucket_name=config_var.get("log_bucket"),
            source_file_name=logfilepath,
            destination_blob_name=logfile,
            keyfile_path=config_var.get("gcp_creds"),
        )

        db.close_connection()
        sys.exit(0)

    except Exception as e:
        logger.info(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
