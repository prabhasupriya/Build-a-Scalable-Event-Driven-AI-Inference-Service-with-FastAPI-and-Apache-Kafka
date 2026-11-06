"""FastAPI API gateway for asynchronous, event-driven AI inference.

``POST /inference`` validates the request, stores it as ``PENDING`` and publishes
it to Kafka, then returns ``202 Accepted`` immediately. The actual inference is
performed by the separate ``kafka_consumer`` service.
"""
import logging
import time
import uuid
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, HTTPException, Path, Request, status
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.config import Settings
from app.database import DB_ERRORS, Database
from app.kafka_producer import KAFKA_TRANSIENT_ERRORS, KafkaPublisher, build_message_payload
from app.logging_config import configure_logging, correlation_id_var, log_event
from app.models import (
    ErrorResponse,
    InferenceRequest,
    InferenceResponse,
    RequestStatus,
    SubmitResponse,
)

logger = logging.getLogger(__name__)

CORRELATION_HEADER = "X-Correlation-ID"
MAX_CORRELATION_ID_LENGTH = 128


def generate_request_id() -> str:
    """Return a new globally unique request identifier (UUID4)."""
    return str(uuid.uuid4())


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = Settings.from_env()
    configure_logging(settings.service_name, settings.log_level)

    database = Database(settings)
    publisher = KafkaPublisher(settings)
    await database.connect()
    await publisher.start()

    app.state.settings = settings
    app.state.db = database
    app.state.producer = publisher
    log_event(logger, logging.INFO, "api_gateway_started", topic=settings.kafka_topic_requests)
    try:
        yield
    finally:
        await publisher.stop()
        await database.close()
        log_event(logger, logging.INFO, "api_gateway_stopped")


app = FastAPI(
    title="Event-Driven AI Inference API",
    description=(
        "Accepts inference requests, publishes them to Apache Kafka and exposes the "
        "status/result of each request. Inference itself runs asynchronously in a "
        "separate consumer service."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


@app.middleware("http")
async def correlation_and_access_log(request: Request, call_next):
    """Attach a correlation id to every request and log one access line."""
    incoming = request.headers.get(CORRELATION_HEADER, "").strip()
    correlation_id = incoming[:MAX_CORRELATION_ID_LENGTH] or str(uuid.uuid4())
    token = correlation_id_var.set(correlation_id)
    started = time.perf_counter()
    try:
        try:
            response = await call_next(request)
        except Exception as exc:  # noqa: BLE001 - last-resort handler -> clean 500
            log_event(
                logger,
                logging.ERROR,
                "unhandled_exception",
                exc_info=True,
                method=request.method,
                path=request.url.path,
                error=repr(exc),
            )
            response = JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={"detail": "Internal server error"},
            )
        response.headers[CORRELATION_HEADER] = correlation_id
        level = logging.DEBUG if request.url.path == "/health" else logging.INFO
        log_event(
            logger,
            level,
            "http_request_completed",
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            duration_ms=round((time.perf_counter() - started) * 1000, 2),
        )
        return response
    finally:
        correlation_id_var.reset(token)


@app.exception_handler(RequestValidationError)
async def validation_error_handler(request: Request, exc: RequestValidationError):
    log_event(
        logger,
        logging.WARNING,
        "request_validation_failed",
        method=request.method,
        path=request.url.path,
        errors=[{"loc": list(e["loc"]), "msg": e["msg"]} for e in exc.errors()],
    )
    return await request_validation_exception_handler(request, exc)


@app.post(
    "/inference",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=SubmitResponse,
    tags=["inference"],
    summary="Submit an inference request",
    responses={
        422: {"description": "Invalid request body (client error)"},
        503: {"model": ErrorResponse, "description": "Database or Kafka unavailable"},
    },
)
async def submit_inference_request(body: InferenceRequest, request: Request) -> SubmitResponse:
    """Validate, persist as ``PENDING``, publish to Kafka and return ``202`` immediately."""
    state = request.app.state
    request_id = generate_request_id()
    timestamp = time.time()
    payload = build_message_payload(request_id, body.input_data, body.model_id, timestamp)
    log_event(
        logger,
        logging.INFO,
        "inference_request_received",
        request_id=request_id,
        model_id=body.model_id,
        input_length=len(body.input_data),
    )

    # 1) Persist first so GET /inference/{id} works straight away and the consumer
    #    can treat the row as the source of truth for idempotency.
    try:
        await state.db.create_pending(request_id, body.input_data, body.model_id, timestamp)
    except DB_ERRORS as exc:
        log_event(
            logger,
            logging.ERROR,
            "inference_request_store_failed",
            exc_info=True,
            request_id=request_id,
            error=repr(exc),
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database unavailable, please retry later",
        ) from exc

    # 2) Hand the work to Kafka.
    try:
        await state.producer.publish(
            state.settings.kafka_topic_requests,
            payload,
            key=request_id,
            correlation_id=correlation_id_var.get(),
        )
    except KAFKA_TRANSIENT_ERRORS as exc:
        log_event(
            logger,
            logging.ERROR,
            "inference_request_publish_failed",
            exc_info=True,
            request_id=request_id,
            error=repr(exc),
        )
        try:  # best effort: do not leave an orphaned PENDING row behind
            await state.db.mark_failed(request_id, f"Failed to publish to Kafka: {exc!r}")
        except DB_ERRORS:
            log_event(
                logger,
                logging.WARNING,
                "mark_failed_after_publish_error_failed",
                request_id=request_id,
            )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Message broker unavailable, please retry later",
        ) from exc

    return SubmitResponse(
        request_id=request_id, status=RequestStatus.PENDING.value, timestamp=timestamp
    )


@app.get(
    "/inference/{request_id}",
    response_model=InferenceResponse,
    tags=["inference"],
    summary="Get the status / result of an inference request",
    responses={
        404: {"model": ErrorResponse, "description": "Unknown request_id"},
        503: {"model": ErrorResponse, "description": "Database unavailable"},
    },
)
async def get_inference_result(
    request: Request,
    request_id: str = Path(..., min_length=1, max_length=128),
) -> InferenceResponse:
    try:
        record = await request.app.state.db.get_request(request_id)
    except DB_ERRORS as exc:
        log_event(
            logger,
            logging.ERROR,
            "inference_lookup_failed",
            exc_info=True,
            request_id=request_id,
            error=repr(exc),
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database unavailable, please retry later",
        ) from exc

    if record is None:
        log_event(logger, logging.INFO, "inference_request_not_found", request_id=request_id)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Inference request not found"
        )
    return InferenceResponse(**record)


@app.get("/health", tags=["operations"], summary="Liveness / readiness probe")
async def health_check(request: Request) -> JSONResponse:
    """Return 200 when the database and Kafka are reachable, otherwise 503."""
    db = getattr(request.app.state, "db", None)
    producer = getattr(request.app.state, "producer", None)
    database_ok = bool(db and await db.ping())
    kafka_ok = bool(producer and await producer.ping())
    healthy = database_ok and kafka_ok
    return JSONResponse(
        status_code=status.HTTP_200_OK if healthy else status.HTTP_503_SERVICE_UNAVAILABLE,
        content={
            "status": "ok" if healthy else "degraded",
            "service": "api_gateway",
            "dependencies": {
                "database": "ok" if database_ok else "unavailable",
                "kafka": "ok" if kafka_ok else "unavailable",
            },
        },
    )
