import os
from azure.data.tables import TableServiceClient

class tablestorage:
    """Wrapper for Azure Table Storage operations.
    """

    def __init__(self) -> None:
        """Initializes the TableStorage client using environment variables.
        Ensures the target table exists.
        """
        self.conn_str = os.environ["LINEAGE_RECEIVER_STORAGE_CONN_STR"]
        self.table_name = os.environ["EVENT_METADATA_TABLE"]

        self.table_service_client = TableServiceClient.from_connection_string(self.conn_str)
        self.table_service_client.create_table_if_not_exists(self.table_name)
        

    def insert_event_metadata(self, event_row) -> None:
        """Inserts an event metadata row into the configured Azure Table.

        Args:
            event_row (dict): A dictionary representing the row entity.
        """
        table_client = self.table_service_client.get_table_client(self.table_name)
        table_client.create_entity(event_row)
