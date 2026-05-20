# schemas.py
from pydantic import BaseModel, Field
from typing import Literal, Optional, Dict, Any

class DBAction(BaseModel):
    action: Literal["get", "put", "begin", "commit", "rollback"]
    key: Optional[str] = None
    value: Optional[str] = None
    txn_id: Optional[int] = None

class ToolResult(BaseModel):
    ok: bool
    code: str
    message: str
    data: Optional[Dict[str, Any]] = None
