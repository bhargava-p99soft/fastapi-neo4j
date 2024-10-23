from pydantic import BaseModel
from typing import Dict, Any, Optional 

class Rule(BaseModel):
    rule_id: Optional[str] = None
    name: str
    contextual_description: str
    dynamic_properties: Dict[str, Any] = {}

    class Config:
        json_schema_extra = {
           "example": {
                "name": "price",
                "contextual_description": "This is the product price",
                "dynamic_properties": {
                    "type": "varchar",
                    "required": "boolean"
                }
           }
        }

