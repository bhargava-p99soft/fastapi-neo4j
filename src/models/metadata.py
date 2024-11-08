from pydantic import BaseModel
from typing import Dict, Any, Optional 

class Metadata(BaseModel):
    meta_data: Dict[str, Any] = {}

    # class Config:
    #     json_schema_extra = {
    #        "example": {
    #             "dynamic_properties": {
    #                 "type": "varchar",
    #                 "required": "boolean"
    #             }
    #        }
    #     }