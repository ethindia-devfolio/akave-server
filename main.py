from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import os

from akave_client import AkaveClient
from akave_operations import AkaveOperations
from models import DataSourceData

from db import read_db, write_db

app = FastAPI(title="Environmental Data Platform")

akave_client = AkaveClient(
    node_address=os.getenv('NODE_ADDRESS'),
    port=os.getenv('PORT', 3000)
)

akave_ops = AkaveOperations(akave_client)


@app.post("/data/{data_source_name}")
async def receive_data(data_source_name: str, data: DataSourceData):
    """
    Endpoint to receive data from a data source.

    - `data_source_name`: Name of the data source (will be used as bucket name).
    - `data`: Data sent by the data source.
    """
    try:
        # Process the data if needed (e.g., validation, transformation)
        processed_data = data.dict()

        # Pass the data to Akave operations
        result = await akave_ops.store_data(data_source_name, processed_data)

        return JSONResponse(status_code=200, content={"message": "Data stored successfully", "result": result})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/data/{data_source_name}/files")
async def download_data_file(data_source_name: str):
    """
    Endpoint to download a specific data file.

    - `data_source_name`: Name of the data source (bucket name).
    - `file_name`: Name of the file.
    """
    try:
        temp_path = f"/tmp/{data_source_name}"
        akave_client.download_file(data_source_name, temp_path)

        # Send the file back as a response
        from fastapi.responses import FileResponse
        return FileResponse(temp_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # Clean up the temporary file if needed
        if os.path.exists(temp_path):
            os.remove(temp_path)


@app.post("/create")
async def create_data_source(data_source_name: str):
    """
    Endpoint to create a new data source.
    """
    write_db("INSERT INTO data_sources (name) VALUES (%s)", (data_source_name))
    return {"message": "Data source created successfully"}


@app.get("/data_sources")
async def get_data_sources():
    """
    Endpoint to get all data sources.
    """
    return read_db("SELECT * FROM data_sources")


@app.post("/data_sources/{data_source_name}")
async def create_data_source_version(data_source_name: str, name: str, transformation_type: str):
    """
    Endpoint to add a new version to a data source.
    """
    write_db("INSERT INTO data_source_versions (data_source_name, name, transformation_type) VALUES (%s, %s, %s)", (data_source_name, name, transformation_type))
    return {"message": "Data source version added successfully"}


@app.get("/data_sources/{data_source_name}/versions")
async def get_data_source_versions(data_source_name: str):
    """
    Endpoint to get all versions of a data source.
    """
    return read_db("SELECT * FROM data_source_versions WHERE data_source_name = %s", (data_source_name))

@app.get("/data_sources/{data_source_name}/versions/{version_name}")
async def visualize_transformation(data_source_name: str, version_name: str, transformation_params: dict):
    """
    Endpoint to visualize the transformation of a specific version of a data source.
    """
    return {"message": "Transformation visualized successfully"}

