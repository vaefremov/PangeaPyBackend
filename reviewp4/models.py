from pydantic import BaseModel
from typing import List

class WellMethodsList(BaseModel):
    well: str
    methods: List[str]

