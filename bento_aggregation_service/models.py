from pydantic import BaseModel, Field

__all__ = [
    "DataTypeServiceListing",
    "DataType",
]


class DataTypeServiceListing(BaseModel):
    label: str | None = None
    queryable: bool
    item_schema: dict = Field(..., alias="schema")
    metadata_schema: dict
    id: str
    count: int | None


class DataType(BaseModel):
    service_base_url: str
    data_type_listing: DataTypeServiceListing
