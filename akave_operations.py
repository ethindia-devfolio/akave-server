# akave_operations.py
import csv
import os
from akave_client import AkaveClient


class AkaveOperations:
    def __init__(self, akave_client: AkaveClient):
        self.client = akave_client

    async def store_data(self, bucket_name: str, file_content: bytes, file_name: str):
        """
        Store data in Akave storage.
        
        Args:
            bucket_name: Name of the bucket (data source)
            file_content: Content of the file in bytes
            file_name: Name of the file to store
            
        Returns:
            dict: Result of the storage operation
        """
        try:
            # Save file content to a temporary file
            print(f'file_name: {file_name}')
            temp_path = f"/tmp/{file_name}"
            try:
                with open(temp_path, "wb") as f:
                    f.write(file_content)
                print(f'Successfully wrote to {temp_path}')
                print(f'File exists: {os.path.exists(temp_path)}')
                print(f'File size: {os.path.getsize(temp_path)}')
            except IOError as io_err:
                print(f'Failed to write file: {str(io_err)}')
                raise
            
            # Upload the file to Akave
            upload_result = self.client.upload_file(
                bucket_name=bucket_name,
                file_path=temp_path
            )
            
            return {
                "status": "success",
                "file_name": file_name,
                "bucket": bucket_name
            }
            
        except Exception as e:
            raise Exception(f"Failed to store data: {str(e)}")
            
        finally:
            # Clean up temporary file
            if os.path.exists(temp_path):
                os.remove(temp_path)
