from fastapi import FastAPI, HTTPException, UploadFile, Form
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import os

from akave_client import AkaveClient
from akave_operations import AkaveOperations

from db import read_db, write_db
from pydantic import BaseModel

app = FastAPI(title="Environmental Data Platform")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

akave_client = AkaveClient(
    node_address='dashing-gopher-upright.ngrok-free.app',
)

akave_ops = AkaveOperations(akave_client)


class DataSourceCreate(BaseModel):
    data_source_name: str


class TransformationCreate(BaseModel):
    name: str
    type: str

class DataSourceVersionCreate(BaseModel):
    name: str
    data_source_name: str
    type: str

@app.post("/data")
async def receive_data(
    file: UploadFile,
    endpoint_name: str = Form(...)
):
    """
    Endpoint to receive CSV file upload from a data source.

    - `file`: The uploaded CSV file
    - `endpoint_name`: Name of the data source (will be used as bucket name)
    """
    try:
        # Read the file content
        file_content = await file.read()
        
        # Store the file name and content in the database
        write_db(
            "INSERT INTO data_sources (name) VALUES (%s)", 
            (endpoint_name,)
        )
        
        # Upload the file content to Akave
        result = await akave_ops.store_data(
            bucket_name=endpoint_name,
            file_content=file_content,
            file_name=file.filename
        )

        return JSONResponse(
            status_code=200, 
            content={
                "message": "File uploaded successfully",
                "upload_result": result
            }
        )
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
async def create_data_source(data: DataSourceCreate):
    """
    Endpoint to create a new data source.
    """
    write_db("INSERT INTO data_sources (name) VALUES (%s)", (data.data_source_name,))
    print(f"Data source created: {data.data_source_name}")
    return {"message": "Data source created successfully"}


@app.get("/data_sources")
async def get_data_sources():
    """
    Endpoint to get all data sources.
    """
    return read_db("SELECT * FROM data_sources")


@app.post("/transformations")
async def create_data_source_version(data: DataSourceVersionCreate):
    """
    Endpoint to add a new version to a data source.
    """
    write_db("INSERT INTO data_sources_versions (data_source_id, name, transformation_type) VALUES ((SELECT id FROM data_sources WHERE name = %s), %s, %s)", (data.data_source_name, data.name, data.type))
    return {"message": "Data source version added successfully"}


@app.get("/data_sources/{data_source_name}/versions")
async def get_data_source_versions(data_source_name: str):
    """
    Endpoint to get all versions of a data source.
    """
    return read_db("SELECT * FROM data_sources_versions WHERE data_source_id = (SELECT id FROM data_sources WHERE name = %s)", (data_source_name,))

@app.get("/data_sources/{data_source_name}/versions/{version_name}")
async def visualize_transformation(data_source_name: str, version_name: str, transformation_params: dict):
    """
    Endpoint to visualize the transformation of a specific version of a data source.
    """
    return {"message": "Transformation visualized successfully"}

