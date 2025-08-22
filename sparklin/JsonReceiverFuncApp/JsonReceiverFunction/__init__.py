import logging
import os
import datetime as dt
import json
import azure.functions as func
from azure.storage.blob import BlobClient
from .tablestorage import tablestorage
from .event import event


def uploadblob(json_input, blob_name, storage_conn_str, lineage_container):
    """Upload a JSON string to Azure Blob Storage.

    Args:
        json_input (str): JSON content to upload.
        blob_name (str): Name of the blob file.
        storage_conn_str (str): Azure Storage connection string.
        container_name (str): Blob container name.
    """
    try:
        blob_client = BlobClient.from_connection_string(storage_conn_str, container_name=lineage_container, blob_name=blob_name)
        blob_client.upload_blob(json_input, overwrite=True)
    except Exception as blob_error:
        logging.error(f"[__init__.py] [ERROR] Blob upload failed: {blob_error}")
        raise


def main(req: func.HttpRequest) -> func.HttpResponse:
    """Azure Function triggered by an HTTP POST request containing OpenLineage JSON.

    - Uploads the payload as a blob.
    - Logs metadata in Azure Table Storage if event matches required criteria.

    Args:
        req (func.HttpRequest): Incoming HTTP request.

    Returns:
        func.HttpResponse: Result of processing.
    """
    try:
        logging.info("[__init__.py] Http Trigger function kicked off.")

        storage_conn_str = os.environ["LINEAGE_RECEIVER_STORAGE_CONN_STR"]
        lineage_container = os.environ["EVENT_LINEAGE_CONTAINER"]

        data = req.get_json()
        logging.info(f"[__init__.py] Payload received: {json.dumps(data)[:500]}")

        event_type = data.get("eventType")
        run_id = data.get("run", {}).get("runId")
        notebook_name = data.get("job", {}).get("name", "no_notebook").split('.')[0]
        job_name = data.get("job", {}).get("name", "").lower()

        current_time_stamp = dt.datetime.utcnow().strftime("%Y%m%d%H%M%S")
        file_name = f"{run_id}_{notebook_name}_{current_time_stamp}.json"
        file_path = f"{lineage_container}/{file_name}"
        
        
        # Define job name patterns to filter relevant lineage jobs
        patterns = [
            "create_table",
            "create_view_statement",
            "create_table_as_select_statement",
            "insert_into_statement",
            "save_into_data_source_command",
            "merge_into_table"
            "create_gold_table",
            "create_silver_table",
            "create_bronze_table"
        ]        
        
        if event_type == "COMPLETE" and job_name and any(pattern in job_name for pattern in patterns) :
            try:
                uploadblob(json.dumps(data), file_name, storage_conn_str, lineage_container)
                logging.info(f"[__init__.py] Blob uploaded successfully: {file_path}")
            except Exception as blob_error:
                logging.error(f"[__init__.py] [ERROR] Blob upload failed: {blob_error}")
                return func.HttpResponse(f"[__init__.py] [ERROR] Error uploading blob: {str(blob_error)}", status_code=500)

            # Record event in Table Storage for monitoring
            event_row = event("HRSI", file_name) # , file_path
            event_row.Status = "Unprocessed"
            event_row.RetryCount = 3
            event_row.FilePath = file_path
            event_row.isArchived = False
            event_row.Message = ""

            table_storage = tablestorage()
            table_storage.insert_event_metadata(event_row.__dict__)

            return func.HttpResponse("[__init__.py] Event processed and stored.", status_code=200)

        else:
            logging.info(f"[__init__.py] Ignored event: eventType={event_type}, job_name={job_name}")
            return func.HttpResponse("[__init__.py] Event not COMPLETE or ClassName Not Matched.", status_code=204)

    except Exception as e:
        logging.error(f"[__init__.py] [ERROR] Error processing request: {e}")
        return func.HttpResponse(f"[__init__.py] [ERROR] Error: {str(e)}\nPayload: {req.get_body()}", status_code=500)
