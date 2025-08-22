import json
import logging
import os
import azure.functions as func
import urllib.parse
from .json_parser import main as parse_lineage
from azure.data.tables import TableServiceClient, UpdateMode
from azure.storage.blob import BlobServiceClient

def main(event: func.EventGridEvent):
    """Azure Event Grid-triggered function that processes lineage metadata from a blob storage event.

    This function performs the following steps:
    - Filters out irrelevant internal Azure events.
    - Extracts the blob URL and retrieves the blob content.
    - Checks and updates the corresponding event metadata in the Azure Table Storage.
    - Parses the blob to extract lineage information.
    - Updates lineage details in Azure Table Storage if processing is successful.

    Args:
        event (func.EventGridEvent): The incoming Event Grid event containing blob information.
    """
    logging.info("[__init__.py] Event Grid Trigger received.")
    
    subject = event.subject
    if "/azure-webjobs-hosts/" in subject:
        logging.info(f"[__init__.py] Ignored event from internal Azure control path: {subject}")
        return
    
    event_data = event.get_json()
    blob_url = event_data.get('url')
    if not blob_url:
        logging.warning("[__init__.py] No blob URL found in event data.")
        return

    parsed = urllib.parse.urlparse(blob_url)
    path_parts = parsed.path.lstrip("/").split("/", 1)
    container_name = path_parts[0]
    blob_name = path_parts[1]

    storage_conn_str = os.environ["LINEAGE_RECEIVER_STORAGE_CONN_STR"]
    lineage_event_table_name = os.environ["EVENT_METADATA_TABLE"]
    lineage_details_table_name = os.environ["LINEAGE_DETAILS_TABLE"]
    
    table_service = TableServiceClient.from_connection_string(conn_str=storage_conn_str)
    event_metadata_table = table_service.get_table_client(lineage_event_table_name)
    
    try:
        metadata_entity = event_metadata_table.get_entity(partition_key='HRSI', row_key=blob_name)
        logging.info(f"[__init__.py] Found EventMetadata entity: {metadata_entity}")
    except Exception as e:
        logging.error(f"[__init__.py] EventMetadata entry not found for {blob_name}: {e}")
        return
    
    status = metadata_entity.get("Status")
    if status != "Unprocessed":
        logging.info(f"[__init__.py] Skipping processing. Status is '{status}'.")
        return
    
    blob_service_client = BlobServiceClient.from_connection_string(storage_conn_str)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_bytes = blob_client.download_blob().readall()
    blob_str = blob_bytes.decode('utf-8')
    
    result = parse_lineage(blob_str)
    
    metadata_entity["Status"] = result["status"]
    metadata_entity["Message"] = result["message"]

    event_metadata_table.update_entity(mode=UpdateMode.MERGE, entity=metadata_entity)
    
    if metadata_entity["Status"] == "Processed" and result["details"]:     
        table_service.create_table_if_not_exists(lineage_details_table_name)
        lineage_details_table = table_service.get_table_client(lineage_details_table_name)
        lineage_details_table.create_entity({
            "PartitionKey": 'HRSI', # str
            "RowKey": blob_name, # str
            "process_name": result["details"]["process_name"], # str
            "input_datasets": json.dumps(result["details"]["input_datasets"]), # list
            "output_datasets": json.dumps(result["details"]["output_datasets"]), # list
            "intermediate_process": result["details"]["intermediate_process"], # str
            "input_tables": result["details"]["input_tables"], # str (join)
            "output_tables": result["details"]["output_tables"], # str (join)
            "input_columns": json.dumps(result["details"]["input_columns"]), # list
            "output_columns": json.dumps(result["details"]["output_columns"]), # list
            "derived_columns": json.dumps(result["details"]["derived_columns"]), # list
            "joinconditions": json.dumps(result["details"]["joinconditions"]), # list
            "isdelta": result["details"]["isdelta"] # bool
        })

