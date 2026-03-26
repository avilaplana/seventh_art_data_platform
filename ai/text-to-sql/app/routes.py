import logging
from fastapi import APIRouter
from pydantic import BaseModel, Field
from typing import List, Any, Optional
from .compiler.runner import SQLAgent

logger = logging.getLogger(__name__)

router = APIRouter()
runner = SQLAgent()

class Response(BaseModel):
    columns: List[str]
    rows: List[List[Any]]

class QueryResponse(BaseModel):
    sql: str
    result: Optional[Response]
    error: Optional[str]
    metrics: dict

class QueryRequest(BaseModel):
    question: str = Field(..., min_length=5, max_length=500)
    model: str
    version: int = Field(..., ge=1, le=10)

@router.post("/query", response_model=QueryResponse)
def query(request: QueryRequest):
    logger.info(
        "Received query: %s for model: %s version: %s",
        request.question,
        request.model,
        request.version,
    )
    result = runner.run(request.question, model=request.model, version=request.version)
    return QueryResponse(
        sql=result.get("sql"),
        result=result.get("result"),
        error=result.get("error"),
        metrics=result.get("metrics"),
    )