from pydantic import BaseModel, Field


class DataSourceData(BaseModel):
    """
    Model representing data from a data source.

    Adjust the fields according to the structure of your environmental data.
    For example, if your data includes temperature and humidity:
    """
    timestamp: str = Field(..., description="Timestamp of the data point")
    temperature: float = Field(..., description="Temperature value")
    humidity: float = Field(..., description="Humidity value")
    # Add more fields as per your data schema
