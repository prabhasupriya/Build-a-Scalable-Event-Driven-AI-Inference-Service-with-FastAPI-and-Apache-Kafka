"""Pydantic request / response models for the API gateway."""
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

DEFAULT_MODEL_ID = "default_model"
MAX_INPUT_LENGTH = 10_000


class RequestStatus(str, Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class InferenceRequest(BaseModel):
    """Body of ``POST /inference``."""

    model_config = ConfigDict(
        str_strip_whitespace=True,
        json_schema_extra={
            "example": {"input_data": "A photo of a cat on a sofa", "model_id": "resnet50"}
        },
    )

    input_data: str = Field(
        ...,
        min_length=1,
        max_length=MAX_INPUT_LENGTH,
        description="Text to analyse, or a URL pointing at the input (e.g. an image).",
    )
    model_id: Optional[str] = Field(
        default=DEFAULT_MODEL_ID,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:/\-]*$",
        description="Identifier of the model that should serve the request.",
    )

    @field_validator("model_id", mode="after")
    @classmethod
    def _default_model_id(cls, value: Optional[str]) -> str:
        return value or DEFAULT_MODEL_ID


class SubmitResponse(BaseModel):
    """Body returned with ``202 Accepted`` by ``POST /inference``."""

    request_id: str
    status: str = RequestStatus.PENDING.value
    timestamp: float


class InferenceResponse(BaseModel):
    """Body returned by ``GET /inference/{request_id}``."""

    request_id: str
    status: str
    input_data: str
    output_data: Optional[str] = None
    timestamp: float
    model_id: Optional[str] = None
    error_message: Optional[str] = None
    updated_at: Optional[float] = None


class ErrorResponse(BaseModel):
    detail: str
