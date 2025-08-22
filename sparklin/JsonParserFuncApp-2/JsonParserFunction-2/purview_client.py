import os
import requests
import logging

class PurviewClient:
    
    def __init__(self):
        """Initialize the Purview client with credentials from environment variables.

        This constructor retrieves necessary Azure credentials and Purview API configuration
        from environment variables, then obtains an access token using these credentials.

        Environment Variables Required:
            - TENANT_ID: Azure Active Directory tenant ID
            - CLIENT_ID: Azure app client ID
            - CLIENT_SECRET: Azure app client secret
            - PURVIEW_RESOURCE: Azure resource ID for Purview
            - PURVIEW_API_URL: Base URL for the Purview API
        """
        self.tenant_id = os.environ["TENANT_ID"]
        self.client_id = os.environ["CLIENT_ID"]
        self.client_secret = os.environ["CLIENT_SECRET"]
        self.resource = os.environ["PURVIEW_RESOURCE"]
        self.api_url = os.environ["PURVIEW_API_URL"]
        self.token = self.get_access_token()

    def get_access_token(self):
        """Request an OAuth2 access token using client credentials.

        This method authenticates with Azure Active Directory using the client credentials
        grant type and returns a valid bearer token to be used in subsequent API calls.

        Returns:
            str: A valid OAuth2 access token.
        """
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
        # DEBUG
        logging.info("@PURVIEW_CLIENT - [BEGIN] PURVIEW_CLIENT.PY")
        
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

        payload = {
            "guid": "-1",
            "typeName": f"dataset_process_inputs" if direction == "input" else f"process_dataset_outputs",
            "end1": {
                "typeName": source_type,
                "uniqueAttributes": {
                    "qualifiedName": f"https://app.fabric.microsoft.com/groups/{workspace_id}/" + 
                                    (f"lakehouses" if direction == "input" else f"synapsenotebooks") +
                                    f"/{source_guid}"
                }
            },
            "end2": {
                "typeName": target_type,
                "uniqueAttributes": {
                    "qualifiedName": f"https://app.fabric.microsoft.com/groups/{workspace_id}/" +
                                    (f"synapsenotebooks" if direction == "input" else f"lakehouses") +
                                    f"/{target_guid}"
                }
            }
        }
        
        url = f"{self.api_url}/datamap/api/atlas/v2/relationship"
        logging.info(f"@PURVIEW_CLIENT - Sending lineage {direction.upper()} to Purview...")
        response = requests.post(url, headers=headers, json=payload)
        
        # DEBUG # BUG
        logging.info(f"@PURVIEW_CLIENT - Response API REST Purview: {response}")
        
        if response.status_code in (200, 201):
            logging.info(f"@PURVIEW_CLIENT - Lineage {direction.upper()} created successfully.")
        elif response.status_code == 409:
            logging.info(f"@PURVIEW_CLIENT - Lineage {direction.upper()} already exists. No action taken.")
        else:
            logging.error(f"@PURVIEW_CLIENT - [ERROR] {direction.upper()} lineage failed: {response.status_code} - {response.text}")
            response.raise_for_status()
        
        # DEBUG
        logging.info("@PURVIEW_CLIENT - Lineage creation completed.")
        
        # DEBUG
        logging.info("@PURVIEW_CLIENT - [END] PURVIEW_CLIENT.PY")
        
        # DEBUG
        logging.info("@PURVIEW_CLIENT - [RETURN CALL] PURVIEW_CLIENT.PY")
        
        return response.status_code, response.text

