import os
import requests


class AkaveClient:
    def __init__(self, node_address=None):
        """
        Initialize the Akave client with the provided configuration.
        If any parameter is not provided, it will try to read from environment variables.  # noqa: E501

        Args:
            node_address (str): Akave node address.
            port (int): API server port.
        """
        self.node_address = node_address or os.getenv('NODE_ADDRESS')

        if not self.node_address:
            raise ValueError("NODE_ADDRESS is required.")

        self.base_url = f"https://{self.node_address}"
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
        })

    def create_bucket(self, bucket_name):
        """
        Create a new bucket for file storage.

        Args:
            bucket_name (str): Name of the bucket to create.

        Returns:
            dict: Response data.
        """
        url = f"{self.base_url}/buckets"
        payload = {
            "bucketName": bucket_name
        }
        response = self.session.post(url, json=payload)
        return self._handle_response(response)

    def list_buckets(self):
        """
        Retrieve a list of all buckets.

        Returns:
            dict: Response data.
        """
        url = f"{self.base_url}/buckets"
        response = self.session.get(url)
        return self._handle_response(response)

    def view_bucket(self, bucket_name):
        """
        Get details of a specific bucket.

        Args:
            bucket_name (str): Name of the bucket.

        Returns:
            dict: Response data.
        """
        print(f'base_url: {self.base_url}')
        url = f"{self.base_url}/buckets/{bucket_name}"
        response = self.session.get(url)
        return self._handle_response(response)

    def list_files(self, bucket_name):
        """
        List all files in a specific bucket.

        Args:
            bucket_name (str): Name of the bucket.

        Returns:
            dict: Response data.
        """
        url = f"{self.base_url}/buckets/{bucket_name}/files"
        response = self.session.get(url)
        return self._handle_response(response)

    def get_file_info(self, bucket_name, file_name):
        """
        Get metadata about a specific file.

        Args:
            bucket_name (str): Name of the bucket.
            file_name (str): Name of the file.

        Returns:
            dict: Response data.
        """
        url = f"{self.base_url}/buckets/{bucket_name}/files/{file_name}"
        response = self.session.get(url)
        return self._handle_response(response)

    def upload_file(self, bucket_name, file_path=None):
        """
        Upload a file to a specific bucket.

        You can provide either a file path or a file-like object.

        Args:
            bucket_name (str): Name of the bucket.
            file_path (str): Path to the file to upload.

        Returns:
            dict: Response data.
        """
        url = f"{self.base_url}/buckets/{bucket_name}/files"
        print(f'file_path: {file_path}')
        if file_path:
            with open(file_path, 'rb') as f:
                files = {'file': f}
                os.system(f'node main.js {bucket_name} {file_path}')
        else:
            raise ValueError("Either file_path or file_obj must be provided.")

        response = {
            "status": "success",
            "file_name": file_path,
            "bucket": bucket_name
        }
        return response


    def download_file(self, bucket_name, file_name, destination_path):
        """
        Download a file from a specific bucket.

        Args:
            bucket_name (str): Name of the bucket.
            file_name (str): Name of the file.
            destination_path (str): Path to save the downloaded file.

        Returns:
            None
        """
        url = f"{self.base_url}/buckets/{bucket_name}/files/{file_name}/download"  # noqa: E501
        response = self.session.get(url, stream=True)
        if response.status_code == 200:
            with open(destination_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
        else:
            error_response = response.json()
            raise Exception(
                f"Error: {error_response.get('error', 'Unknown error')}")

    def _handle_response(self, response):
        """
        Handle the HTTP response, checking for errors and returning JSON data.

        Args:
            response (requests.Response): The HTTP response object.

        Returns:
            dict: The JSON response data.

        Raises:
            Exception: If the response contains an error.
        """
        content_type = response.headers.get('Content-Type', '')
        if 'application/json' in content_type:
            data = response.json()
            if data.get('success'):
                return data.get('data')
            else:
                raise Exception(f"Error: {data.get('error', 'Unknown error')}")
        else:
            response.raise_for_status()
