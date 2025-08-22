import json
import os
import logging
from .purview_client import PurviewClient

def extract_dataset_guid(uri: str) -> str:
    """Extract the dataset GUID from a URI.

    The function assumes the GUID is the second segment in the URI when split by '/'.

    Args:
        uri (str): The URI containing the dataset GUID.

    Returns:
        str: The extracted dataset GUID.
    """
    return uri.split('/')[1]

def extract_notebook_guid(props: dict) -> str:
    """Extract the notebook GUID from a metadata dictionary.

    The function retrieves the value associated with the key
    'trident.artifact.id', which is expected to represent the notebook GUID.

    Args:
        props (dict): A dictionary containing notebook-related metadata.

    Returns:
        str: The notebook GUID if present, otherwise None.
    """
    return props.get("trident.artifact.id")

def extract_workspace_id(props: dict) -> str:
    """Extract the workspace ID from a metadata dictionary.

    The function retrieves the value associated with the key
    'trident.artifact.workspace.id', which represents the workspace identifier.

    Args:
        props (dict): A dictionary containing workspace-related metadata.

    Returns:
        str: The workspace ID if present, otherwise None.
    """
    return props.get("trident.artifact.workspace.id")

def extract_notebook_name(props: dict) -> str:
    """Extract the notebook name from a metadata dictionary.

    The function retrieves the value associated with the key
    'spark.synapse.context.notebookname', which is expected to represent the notebook name.

    Args:
        props (dict): A dictionary containing notebook-related metadata.

    Returns:
        str: The notebook name if present, otherwise None.
    """
    return props.get("spark.synapse.context.notebookname", "Unknown Process")

def extract_table_name_from_path(path: str) -> str:
    """Return the table or file name extracted from a dataset path.

    The function parses the path to extract the table or file name.
    If the path contains 'Files', it returns the file name; otherwise,
    it returns the last segment (typically the table name).

    Args:
        path (str): The full path to the dataset.

    Returns:
        str: The extracted table or file name.
    """
    parts = path.strip('/').split('/')
    if 'Files' in parts:
        return parts[-1]  # e.g., "CURRENCY_202309200845.parquet"
    elif 'Tables' in parts:
        return parts[-1]  # e.g., "currency"
    return "unknown"

def extract_derived_columns(column_lineage: dict) -> dict:
    """Extracts a mapping of output columns to their source input columns.

    Parses the 'columnLineage' facet of an OpenLineage dataset to build a 
    dictionary where each output column is associated with a list of input 
    columns that contribute to its value.

    Args:
        column_lineage (dict): A dictionary representing the 'columnLineage' 
            facet, containing output columns and their input field dependencies.

    Returns:
        dict: A mapping where keys are output column names and values are lists 
        of input column names.
    """
    mapping = {}
    if not column_lineage or "fields" not in column_lineage:
        return mapping

    for sink_col, details in column_lineage["fields"].items():
        sources = []
        for field in details.get("inputFields", []):
            source_field = field.get("field")
            if source_field:
                sources.append(source_field)
        mapping[sink_col] = sources
    return mapping

def main(blob_str: str) -> dict:
    # logging.info("Blob trigger activated")
    
    # DEBUG
    logging.info("@JSON_PARSER - [BEGIN] JSON_PARSER.PY")
    
    try:
        data = json.loads(blob_str)
        logging.info("@JSON_PARSER - Successfully parsed JSON.")
    except json.JSONDecodeError as e:
        logging.error(f"@JSON_PARSER - JSON decoding failed: {e}")
        return {"status": 400, "message": str(e), "details": None}

    # props = data["run"]["facets"]["spark_properties"]["properties"]
    props = data.get("run", {}).get("facets", {}).get("spark_properties", {}).get("properties", {})
    notebook_guid = extract_notebook_guid(props)
    workspace_id = extract_workspace_id(props)
    notebook_name = extract_notebook_name(props)

    purview = PurviewClient()
    response_status = None
    
    input_datasets = []
    output_datasets = []
    input_tables = []
    output_tables = []
    input_columns = []
    output_columns = []
    derived_columns = {}
    joinconditions = []
    isdelta = False
    
    # DEBUG
    logging.info("@JSON_PARSER - Entering for direction, datasets loop")
    
    for direction, datasets in [("input", data.get("inputs", [])), ("output", data.get("outputs", []))]:
        for ds in datasets:
            # DEBUG
            logging.info(f"@JSON_PARSER - Processing {direction} dataset: {ds.get('name', 'Unknown Dataset')}")
            
            # DEBUG
            logging.info("@JSON_PARSER - Extracting ds_guid, source_guid, target_guid, source_type, target_type")
            
            ds_guid = extract_dataset_guid(ds["name"])            
            source_guid = ds_guid if direction == "input" else notebook_guid
            target_guid = notebook_guid if direction == "input" else ds_guid
            source_type = "fabric_lakehouse" if direction == "input" else "fabric_synapse_notebook"
            target_type = "fabric_synapse_notebook" if direction == "input" else "fabric_lakehouse"

            # DEBUG
            logging.info("@JSON_PARSER - Extraction successful")
            
            # DEBUG
            logging.info("@JSON_PARSER - Creating lineage in Purview.")
            logging.info("@JSON_PARSER - [CALL] PURVIEW_CLIENT.PY")

            status_code, message = purview.create_lineage(
                source_guid=source_guid,
                source_type=source_type,
                target_guid=target_guid,
                target_type=target_type,
                workspace_id=workspace_id,
                direction=direction
            )
            
            # DEBUG
            logging.info("@JSON_PARSER - [RETURN CALL] PURVIEW_CLIENT.PY")
            logging.info("@JSON_PARSER - Lineage creation successful or failed.")
            
            # DEBUG
            logging.info(f"@JSON_PARSER - Response status code: {status_code}, message: {message}")
            
            response_status = status_code if response_status is None else max(response_status, status_code)
            
            if direction == "input":
                # DEBUG
                logging.info("@JSON_PARSER - [BEGIN] IF DIRECTION == 'input'")
                logging.info("@JSON_PARSER - => Creation of LineageDetails attributes")
                
                input_datasets.append(ds_guid)
                table_name = extract_table_name_from_path(ds["name"])
                input_tables.append(table_name)
                
                schema_facet = ds.get("facets", {}).get("schema", {})
                input_columns.extend([f["name"] for f in schema_facet.get("fields", [])])
                
                # DEBUG
                logging.info("@JSON_PARSER - [END] IF DIRECTION == 'input'")
            else:
                # DEBUG
                logging.info("@JSON_PARSER - [BEGIN] IF DIRECTION == 'output'")
                logging.info("@JSON_PARSER - => Creation of LineageDetails attributes")
                
                output_datasets.append(ds_guid)
                table_name = extract_table_name_from_path(ds["name"])
                output_tables.append(table_name)
                
                schema_facet = ds.get("facets", {}).get("schema", {})
                output_columns.extend([f["name"] for f in schema_facet.get("fields", [])])

                column_lineage_facet = ds.get("facets", {}).get("columnLineage", {})
                derived_columns.update(extract_derived_columns(column_lineage_facet))

                joinconditions = ds.get("joinColumns", [])
                isdelta = ds.get("isDelta", False)
                
                # DEBUG
                logging.info("@JSON_PARSER - [END] IF DIRECTION == 'output'")
    
    # DEBUG
    logging.info("@JSON_PARSER - Creation of 'details' dictionary for LineageDetails")
    
    details = {
        "process_name": notebook_name, 
        "input_datasets": input_datasets,
        "output_datasets": output_datasets,
        "intermediate_process": notebook_guid,        
        "input_tables": ",".join(input_tables),
        "output_tables": ",".join(output_tables),
        "input_columns": input_columns,
        "output_columns": output_columns,
        "derived_columns": derived_columns,
        "joinconditions": joinconditions,
        "isdelta": isdelta
    }
    
    # DEBUG
    logging.info("@JSON_PARSER - [END] JSON_PARSER.PY")
    
    # DEBUG
    logging.info("@JSON_PARSER - [RETURN CALL] JSON_PARSER.PY")
    
    return {
        "status": "Processed" if response_status in [200, 201, 409] else "Failed",
        "message": "SUCCESS" if response_status in [200, 201, 409] else f"[{response_status}] {message}",
        "details": details if response_status in [200, 201] else None # 409
    }
    
