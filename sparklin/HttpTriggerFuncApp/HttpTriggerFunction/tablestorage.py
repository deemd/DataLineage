import os
from azure.data.tables import TableServiceClient

class tablestorage:
    
    def __init__(self) -> None:
        self.connstr = os.environ["LINEAGE_STORAGE_CONN_STR"]
        self.tablename = os.environ["LINEAGE_EVENT_TABLE"]


        self.table_service_client = TableServiceClient.from_connection_string(self.connstr)
        self.table_service_client.create_table_if_not_exists(self.tablename)
        

    def insertEventMetadata(self, eventrow) -> None:
        table_client = self.table_service_client.get_table_client(self.tablename)
        table_client.create_entity(eventrow)
