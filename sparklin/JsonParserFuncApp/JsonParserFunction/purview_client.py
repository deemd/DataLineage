import os
import requests
import logging

class PurviewClient:
    def __init__(self):
        self.tenant_id = os.environ["TENANT_ID"]
        self.client_id = os.environ["CLIENT_ID"]
        self.client_secret = os.environ["CLIENT_SECRET"]
        self.resource = os.environ["PURVIEW_RESOURCE"]
        self.api_url = os.environ["PURVIEW_API_URL"]

        self.token = self.get_access_token()

    def get_access_token(self):
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/token"
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "resource": self.resource
        }

        response = requests.post(url, data=payload)
        response.raise_for_status()
        return response.json()["access_token"]

    def create_lineage(self, source_guid, source_type, target_guid, target_type, workspace_id, direction):
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

        payload = {
            "typeName": f"dataset_process_{direction}s",
            "end1": {
                "guid": target_guid if direction == "input" else source_guid,
                "typeName": target_type,
                "uniqueAttributes": {
                    "qualifiedName": f"https://app.fabric.microsoft.com/groups/{workspace_id}/" + (
                        f"synapsenotebooks/{source_guid}" if direction == "input" else f"lakehouses/{source_guid}"
                    )
                }
            },
            "end2": {
                "guid": source_guid if direction == "input" else target_guid,
                "typeName": source_type,
                "uniqueAttributes": {
                    "qualifiedName": f"https://app.fabric.microsoft.com/groups/{workspace_id}/" + (
                        f"lakehouses/{source_guid}" if direction == "input" else f"synapsenotebooks/{target_guid}"
                    )
                }
            },
            "label": f"__Process.{direction}s",
            "status": "ACTIVE"
        }

        url = f"{self.api_url}/datamap/api/atlas/v2/relationship"
        logging.info(f"Sending lineage {direction.upper()} to Purview...")
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 200 or response.status_code == 201:
            logging.info(f"Lineage {direction.upper()} created successfully")
        else:
            logging.error(f"Error sending lineage {direction.upper()}: {response.status_code} - {response.text}")
            response.raise_for_status()
