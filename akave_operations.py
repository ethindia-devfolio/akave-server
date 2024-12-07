# akave_operations.py
import csv
import os
from akave_client import AkaveClient


class AkaveOperations:
    def __init__(self, akave_client: AkaveClient):
        self.akave_client = akave_client

    async def store_data(self, data_source_name: str, data: dict):
        """
        Store the data into Akave object storage.

        - Create a bucket for the data source if it doesn't exist.
        - Upload the data as a file.

        Args:  
            data_source_name (str): Name of the data source (bucket name).
            data (dict): Data to store.

        Returns:  
            dict: Result of the upload.
        """
        # Ensure the bucket exists
        try:
            self.akave_client.view_bucket(data_source_name)
        except Exception:
            # Bucket doesn't exist; create it
            self.akave_client.create_bucket(data_source_name)

        data_file_path = f"/tmp/{data_source_name}_{
            data['timestamp'].replace(':', '-')}.csv"
        with open(data_file_path, 'w') as f:
            writer = csv.writer(f)
            writer.writerow(data.keys())
            writer.writerows(data.values())

        # Upload the file to the bucket
        try:
            result = self.akave_client.upload_file(
                data_source_name, file_path=data_file_path)
            return result
        finally:
            # Clean up the temporary file
            if os.path.exists(data_file_path):
                os.remove(data_file_path)
