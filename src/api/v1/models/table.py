from pydantic import BaseModel
from typing import Dict, Any, Optional 


class Table(BaseModel):
    table_id: Optional[str] = None
    name: str
    dynamic_properties: Dict[str, Any] = {}

    class Config:
        json_schema_extra = {
            "example": {
                "name": "Products",
                "dynamic_properties": {
                    "no_of_colums": 4
                }
            }
        }

