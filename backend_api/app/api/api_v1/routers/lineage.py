from fastapi import APIRouter, Depends
from typing import List, Dict, Any
from app.lineage.graph import lineage_engine

router = APIRouter()

@router.get("/mapping", response_model=List[Dict[str, Any]])
def get_data_mapping():
    """
    Get flattened Data Mapping records for visual compliance tables.
    Matches the schema required by Enterprise Data Map logic.
    """
    return lineage_engine.get_data_mapping()
