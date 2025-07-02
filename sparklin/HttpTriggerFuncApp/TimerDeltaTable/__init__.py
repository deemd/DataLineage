import os
import json
import logging
from datetime import datetime
from azure.storage.blob import BlobServiceClient

import azure.functions as func

def main(mytimer: func.TimerRequest) -> None:
    logging.info("=== DeltaTable TimerTrigger started ===")
    try:
        # 1. Récupérer les settings du container et de la connexion (comme ta fonction HTTP Trigger)
        lineageContainerStr = os.environ["LINEAGE_STORAGE_CONN_STR"]
        lineageContainer = os.environ["LINEAGE_CONTAINER"]
        logging.info(f"Variables d'environnement récupérées : CONN_STR={bool(lineageContainerStr)}, CONTAINER={lineageContainer}")
        
        # Création du client blob
        blob_service_client = BlobServiceClient.from_connection_string(lineageContainerStr)
        container_client = blob_service_client.get_container_client(lineageContainer)
        logging.info(f"Accès au container : {lineageContainer}")

        # 2. Chercher dynamiquement toutes les DeltaTables du container
        delta_table_paths = set()
        all_blobs = container_client.list_blobs()
        logging.info(f"Liste de tous les blobs détectés dans le container :")
        blob_count = 0
        for blob in all_blobs:

            logging.info(f"Blob : {blob.name}")
            blob_count += 1
            if "/_delta_log/" in blob.name and blob.name.endswith(".json"):
                table_path = blob.name.split("/_delta_log/")[0]
                delta_table_paths.add(table_path)
        logging.info(f"{blob_count} blobs inspectés. {len(delta_table_paths)} DeltaTable(s) trouvée(s) : {delta_table_paths}")
        delta_table_paths = list(delta_table_paths)

        # 3. Pour chaque table, scanner les fichiers _delta_log/*.json
        event_count = 0
        for table_path in delta_table_paths:
            delta_log_prefix = f"{table_path}/_delta_log/"
            logging.info(f"Scan du dossier DeltaLog : {delta_log_prefix}")
            blobs = container_client.list_blobs(name_starts_with=delta_log_prefix)
            for blob in blobs:
                if blob.name.endswith('.json'):
                    try:
                        logging.info(f"Traitement de {blob.name}")
                        blob_client = container_client.get_blob_client(blob.name)
                        delta_json = json.loads(blob_client.download_blob().readall())
                        
                        # --- Extraction des infos principales ---
                        commit_info = delta_json.get('commitInfo', {})
                        event_time = commit_info.get('timestamp')
                        if event_time:
                            event_time = datetime.utcfromtimestamp(event_time/1000).isoformat() + "Z"
                        else:
                            event_time = datetime.utcnow().isoformat() + "Z"
                        
                        # ID du job (runId)
                        run_id = commit_info.get('operationMetrics', {}).get('commitId')
                        if not run_id:
                            run_id = os.path.basename(blob.name).replace(".json", "")
                        
                        # Nom de la table
                        table_name = table_path.replace("/", "_")
                        
                        # Type d'opération (write, merge, etc.)
                        operation = commit_info.get('operation', 'UNKNOWN')
                        
                        # Inputs/outputs - ici à affiner selon ce que tu veux parser des logs
                        inputs = []
                        outputs = [{
                            "namespace": f"abfss://{lineageContainer}@toncompte.dfs.core.windows.net",
                            "name": table_path,
                            "facets": {}
                        }]
                        
                        # Optionnel : lecture d'inputs éventuels
                        if 'read' in delta_json:
                            for read_entry in delta_json['read'].get('reads', []):
                                inputs.append({
                                    "namespace": f"abfss://{lineageContainer}@toncompte.dfs.core.windows.net",
                                    "name": read_entry.get('path', 'unknown'),
                                    "facets": {}
                                })
                        
                        # --- Construction du JSON OpenLineage ---
                        event = {
                            "eventTime": event_time,
                            "producer": "deltatable-lineage-function",
                            "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
                            "eventType": "COMPLETE",
                            "run": {
                                "runId": run_id,
                                "facets": {
                                    "delta_operation": {
                                        "operation": operation
                                    }
                                }
                            },
                            "job": {
                                "namespace": "ton-projet",
                                "name": table_name
                            },
                            "inputs": inputs,
                            "outputs": outputs
                        }
                        
                        # 4. Construire un nom de fichier comme dans ta première fonction
                        currenttimestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
                        output_blob_name = f"deltatable_events/{run_id}_{table_name}_{currenttimestamp}.json"
                        
                        # 5. Écrire le JSON dans le même container
                        logging.info(f"Écriture du JSON dans {output_blob_name}")
                        container_client.upload_blob(name=output_blob_name, data=json.dumps(event, indent=2), overwrite=True)
                        logging.info(f"Event écrit avec succès ! [{output_blob_name}]")
                        event_count += 1
                    
                    except Exception as e:
                        logging.error(f"Erreur lors du traitement de {blob.name}: {repr(e)}")
        logging.info(f"DeltaTable TimerTrigger terminé. {event_count} event(s) écrit(s).")

    except Exception as e:
        logging.error(f"Erreur globale dans la fonction Timer: {repr(e)}")
