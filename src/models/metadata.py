from pydantic import BaseModel
from typing import List

# # model to handle dynamic properties and allow extra fields
# class NodeResponse(BaseModel):
#     custom_id: str
#     database_name: str
#     kind: str
#     column_name: str
#     schema_name: str
#     table_name: str
#     default: Optional[str] = None
#     autoincrement: Optional[str] = None
#     name: str
#     data_type: Optional[str]
#     meta_data: Optional[str] = None
#     comment: Optional[str] = None
#     null: Optional[bool] = None

#     @staticmethod
#     def parse_data_type(value: str) -> str:
#         try:
#             parsed = json.loads(value)  # Try to parse JSON string if needed
#             return str(parsed)
#         except (json.JSONDecodeError, TypeError):
#             return value  # If not a JSON string, just return the value as is

#     class Config:
#         anystr_strip_whitespace = True  # Strip any unnecessary spaces from strings
#         extra = "allow"  # Allow extra fields that are not part of the model

# Response model for returning message and nodes
class SearchResponse(BaseModel):
    message: str
    nodes: List[dict]