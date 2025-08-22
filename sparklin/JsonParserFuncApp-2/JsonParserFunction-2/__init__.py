import json
import logging
import os
import azure.functions as func
import urllib.parse
from .json_parser import main as parse_lineage
from azure.data.tables import TableServiceClient, UpdateMode
from azure.storage.blob import BlobServiceClient

def main(event: func.EventGridEvent):
    logging.info("EventGrid trigger received")
    
    # DEBUG
    logging.info("@__INIT__ - [BEGIN] __INIT__.PY")
    
    # DEBUG
    logging.info("@__INIT__ - GARDE-FOU : Ignorer les événements internes Azure (timer blobs, etc.)")
    
    # 🔍 Garde-fou : ignorer les événements internes Azure (timer blobs, etc.)
    subject = event.subject
    if "/azure-webjobs-hosts/" in subject:
        logging.info(f"@__INIT__ - Ignored event from internal Azure control path: {subject}")
        return
    
    event_data = event.get_json()
    logging.info(f"@__INIT__ - Event data (JSON): {event_data}")
    blob_url = event_data.get('url')
    if not blob_url:
        logging.warning("@__INIT__ - No blob URL found in event data")
        return

    # Extraction du chemin du blob
    parsed = urllib.parse.urlparse(blob_url)
    path_parts = parsed.path.lstrip("/").split("/", 1)
    container_name = path_parts[0]
    blob_name = path_parts[1]

    # Connexion au service Blob Azure
    storage_conn_str = os.environ["LINEAGE_STORAGE_CONN_STR"]
    table_service = TableServiceClient.from_connection_string(conn_str=storage_conn_str)

    event_metadata_table = table_service.get_table_client("EventMetadata")
    try:
        metadata_entity = event_metadata_table.get_entity(partition_key='HRSI', row_key=blob_name)
        logging.info(f"@__INIT__ - Found EventMetadata entity: {metadata_entity}")
    except Exception as e:
        logging.error(f"@__INIT__ - EventMetadata entry not found for {blob_name}: {e}")
        return
    
    # DEBUG
    logging.info("@__INIT__ - Vérification du statut de l'entité EventMetadata")
    
    # Vérification du statut
    status = metadata_entity.get("Status")
    if status != "Unprocessed":
        logging.info(f"@__INIT__ - Skipping processing. Status is '{status}'.")
        return
    
    # DEBUG
    logging.info("@__INIT__ - Lecture du blob JSON (en bytes), décodage UTF-8")
    
    # Lecture du blob JSON (en bytes), décodage UTF-8
    blob_service_client = BlobServiceClient.from_connection_string(storage_conn_str)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_bytes = blob_client.download_blob().readall()
    blob_str = blob_bytes.decode('utf-8')
    
    # DEBUG
    logging.info("@__INIT__ - Parsing lineage")
    logging.info("@__INIT__ - [CALL] JSON_PARSER.PY")
    
    # Traitement
    result = parse_lineage(blob_str)
    
    # DEBUG
    logging.info("@__INIT__ - [RETURN CALL] JSON_PARSER.PY")
    logging.info("@__INIT__ - Lineage parsing completed.")
    
    metadata_entity["Status"] = result["status"]
    metadata_entity["Message"] = result["message"]
    
    # DEBUG
    logging.info(f"@__INIT__ - Updating EventMetadata entity with status: {metadata_entity['Status']} and message: {metadata_entity['Message']}")

    event_metadata_table.update_entity(mode=UpdateMode.MERGE, entity=metadata_entity)
    
    # DEBUG # BUG
    logging.info("@__INIT__ - EventMetadata entity updated successfully.")
    
    # DEBUG
    logging.info("@__INIT__ - Checking if lineage details should be added to LineageDetails table")

    # Si succès, ajouter à LineageDetails
    if metadata_entity["Status"] == "Processed" and result["details"]:
        # DEBUG
        logging.info("@__INIT__ - Adding lineage details to LineageDetails table")          

        table_service.create_table_if_not_exists("LineageDetails")
        lineage_details_table = table_service.get_table_client("LineageDetails")
        lineage_details_table.create_entity({
            "PartitionKey": 'HRSI', # str
            "RowKey": blob_name, # str
            "process_name": result["details"]["process_name"], # str
            "input_datasets": json.dumps(result["details"]["input_datasets"]), # list
            "output_datasets": json.dumps(result["details"]["output_datasets"]), # list
            "intermediate_process": result["details"]["intermediate_process"], # str
            "input_tables": result["details"]["input_tables"], # str (join)
            "output_tables": result["details"]["output_tables"], # str (join)
            "input_columns": json.dumps(result["details"]["input_columns"]),
            "output_columns": json.dumps(result["details"]["output_columns"]),
            "derived_columns": json.dumps(result["details"]["derived_columns"]),
            "joinconditions": json.dumps(result["details"]["joinconditions"]),
            "isdelta": result["details"]["isdelta"]
        })
        
        # DEBUG
        logging.info("@__INIT__ - LineageDetails entity added or updated successfully.")

        
    # DEBUG
    logging.info("@__INIT__ - [END] __INIT__.PY")
