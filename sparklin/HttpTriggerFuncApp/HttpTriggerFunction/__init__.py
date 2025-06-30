import logging
import os
import datetime as dt
import json
import azure.functions as func
from azure.storage.blob import BlobClient, ContainerClient, ContentSettings 
from .tablestorage import tablestorage
from .event import event


def uploadblob(json_in, blobname, conn_str, lin_container):

    container_client = ContainerClient.from_connection_string(conn_str, container_name=lin_container)
    blob = BlobClient.from_connection_string(conn_str, container_name= lin_container, blob_name=blobname)
    BlobClient
    blob.upload_blob(json_in, overwrite=True)
    
    try:
        blob.upload_blob(json_in, overwrite=True)
    except Exception as blob_err:
        logging.error(f"Blob upload failed: {blob_err}")
        raise


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info("http trigger function kicked off")

        lineageContainerStr = os.environ["LINEAGE_STORAGE_CONN_STR"]
        lineageContainer = os.environ["LINEAGE_CONTAINER"]

        data = req.get_json()
        logging.info(f"Payload reçu : {json.dumps(data)[:500]}")  # Limité à 500 caractères

        eventType = data.get("eventType")
        runId = data.get("run", {}).get("runId")
        notebookName = data.get("job", {}).get("name", "")
        facets = data.get("run", {}).get("facets", {})
        
        #logging.info(f"eventType={eventType}, className={className}, runId={runId}, notebookName={notebookName}")
      
        if not notebookName:
            notebookName = "no_notebook"

        notebookName = notebookName.split('.')[0]

        currenttimestamp = dt.datetime.utcnow().strftime("%Y%m%d%H%M%S")
        fileName = f"{runId}_{notebookName}_{currenttimestamp}.json"
        filePath = f"{lineageContainer}/{fileName}"
        
        job_name = data.get("job", {}).get("name", "").lower()
        logging.info(f"Job Name: {job_name}")
        """
        # Motifs associés aux classes Spark recherchées
        patterns = [
        "create_table",
        "create_view_statement",
        "create_table_as_select_statement",
        "insert_into_statement",
        "save_into_data_source_command",
        "merge_into_table"
        "create_gold_table",
        "create_silver_table",
        "create_bronze_table",
        ]
        
        found_patterns = [pattern for pattern in patterns if pattern in job_name]
        logging.info(f"Patterns trouvés dans job_name : {found_patterns}")
        and job_name and any(pattern in job_name for pattern in patterns)
        """
        if eventType == "COMPLETE" :
            try:
                uploadblob(json.dumps(data), fileName, lineageContainerStr, lineageContainer)
                logging.info(f"Blob upload OK : {filePath}")
            except Exception as blob_err:
                logging.error(f"Blob upload failed: {blob_err}")
                return func.HttpResponse(f"Error uploading blob: {str(blob_err)}", status_code=500)

            # code to add row in table storage
            eventrow = event('HRSI', fileName)
            eventrow.Status = 'Unprocessed'
            eventrow.RetryCount = 3
            eventrow.FilepPath = filePath
            eventrow.isArchived = False
            eventrow.Message = ''

            tableStorage = tablestorage()
            tableStorage.insertEventMetadata(eventrow.__dict__)

            return func.HttpResponse("Func App successfully processed http request", status_code=200)

        else:
            logging.info(f"Ignoré : eventType={eventType}")
            return func.HttpResponse("Event not COMPLETE or ClassName Not Matched", status_code=204)

    except Exception as e:
        logging.error(f"Error processing request: {e}")
        return func.HttpResponse(f"Error: {str(e)}\nPayload: {req.get_body()}", status_code=500)
