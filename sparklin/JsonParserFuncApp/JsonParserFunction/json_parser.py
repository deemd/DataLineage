import json
import os
import logging
from .purview_client import PurviewClient

def extract_dataset_guid(uri: str) -> str:
    """Extrait le GUID du lakehouse à partir de l'URI OpenLineage"""
    return uri.split('/')[1]

def extract_notebook_guid(props: dict) -> str:
    return props.get("trident.artifact.id")

def extract_workspace_id(props: dict) -> str:
    return props.get("trident.artifact.workspace.id")

def main(blob):
    logging.info("Blob trigger activated")

    data = json.loads(blob.read())

    props = data["run"]["facets"]["spark_properties"]["properties"]
    notebook_guid = extract_notebook_guid(props)
    workspace_id = extract_workspace_id(props)

    purview = PurviewClient()

    # INPUT
    for input_ds in data.get("inputs", []):
        input_guid = extract_dataset_guid(input_ds["name"])
        purview.create_lineage(
            source_guid=input_guid,
            source_type="fabric_lakehouse",
            target_guid=notebook_guid,
            target_type="fabric_synapse_notebook",
            workspace_id=workspace_id,
            direction="input"
        )

    # OUTPUT
    for output_ds in data.get("outputs", []):
        output_guid = extract_dataset_guid(output_ds["name"])
        purview.create_lineage(
            source_guid=notebook_guid,
            source_type="fabric_synapse_notebook",
            target_guid=output_guid,
            target_type="fabric_lakehouse",
            workspace_id=workspace_id,
            direction="output"
        )
