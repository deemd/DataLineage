import azure.data.tables

class event():
    """Represents an event entity to be stored in Azure Table Storage for lineage tracking.
    """
    
    Status = "Unprocessed"
    Message = ""
    RetryCount = 3
    FilePath = "/openlineage/"
    isArchived = 0

    def __init__(self, team_name: str, file_name: str) -> None: # , file_path:str
        """Initializes a new Event row entity.

        Args:
            team_name (str): Logical team name used as PartitionKey.
            file_name (str): Unique identifier used as RowKey.
        """
        self.PartitionKey = team_name
        self.RowKey = file_name
        # self.Status = "Unprocessed"
        # self.Message = ""
        # self.RetryCount = 3
        # self.FilePath = file_path # "/openlineage/"
        # self.isArchived = False
