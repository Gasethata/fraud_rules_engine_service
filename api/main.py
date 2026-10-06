"""
Main FastAPI application with endpoint definitions.

Handles:
    - API route definitions (/health/live, /health/ready, /metrics, /v1/login, /v1/flagged)
    - Connection pooling for database access
    - Startup initialization (admin user creation)
    - Authentication dependency injection
    - Structured JSON logging with correlation IDs
    - Prometheus metrics
"""

import os
import uuid
import time
import json
import logging
import psycopg2
from psycopg2 import pool
from typing import List, Optional
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import datetime
from passlib.context import CryptContext
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, Depends, HTTPException, Request, Response, status
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

from schemas import Token, LoginRequest, FlaggedTransaction, PaginatedFlaggedTransactions
from auth import authenticate_user, create_access_token, get_current_user

# -------------------------------------------------------
# Structured JSON logging
# -------------------------------------------------------
correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default="")

class StructuredFormatter(logging.Formatter):
    def format(self, record):
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "service": "fraud-rule-engine-api",
            "correlation_id": correlation_id_var.get(""),
            "message": record.getMessage(),
            "logger": record.name,
        }
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_data)

handler = logging.StreamHandler()
handler.setFormatter(StructuredFormatter())
logging.basicConfig(level=logging.INFO, handlers=[handler])
logger = logging.getLogger(__name__)

# -------------------------------------------------------
# Secrets validation - fail fast on missing or default values
# -------------------------------------------------------
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")
JWT_SECRET = os.getenv("JWT_SECRET")

if not ADMIN_USERNAME or not ADMIN_PASSWORD:
    raise ValueError("ADMIN_USERNAME and ADMIN_PASSWORD must be set")

if not JWT_SECRET or JWT_SECRET == "this_is-A_JwT_sEcret":
    raise ValueError("JWT_SECRET must be set to a strong random value (openssl rand -hex 32)")

if ADMIN_PASSWORD == "admin":
    raise ValueError("ADMIN_PASSWORD must not be a default value")

# -------------------------------------------------------
# Database connection pool
# -------------------------------------------------------
db_config = {
    "host": os.getenv("HOST"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "connect_timeout": 5,                        # fail fast if DB unreachable
    "options": "-c statement_timeout=30000",     # kill queries running over 30s
}

for key, value in db_config.items():
    if key not in ("port", "connect_timeout", "options") and value is None:
        raise ValueError(f"Database configuration '{key}' is not set")

# Pool initialised at module level — connections are reused across requests.
# minconn=5 keeps connections warm; maxconn=20 caps total under load.
db_pool = pool.ThreadedConnectionPool(
    minconn=5,
    maxconn=20,
    cursor_factory=RealDictCursor,
    **db_config,
)

from contextlib import contextmanager

@contextmanager
def get_db_connection():
    """Borrow a connection from the pool, return it when done."""
    conn = None
    try:
        conn = db_pool.getconn()
        yield conn
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        logger.error("Database error", extra={"error": str(e)})
        raise
    finally:
        if conn:
            db_pool.putconn(conn)

# -------------------------------------------------------
# Prometheus metrics
# -------------------------------------------------------
REQUEST_COUNT = Counter(
    "api_requests_total",
    "Total API requests",
    ["method", "endpoint", "status_code"],
)
REQUEST_DURATION = Histogram(
    "api_request_duration_seconds",
    "Request duration in seconds",
    ["method", "endpoint"],
)
DB_POOL_AVAILABLE = Gauge(
    "db_pool_connections_available",
    "Number of available connections in the pool",
)

# -------------------------------------------------------
# Admin user seeding
# -------------------------------------------------------
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def create_admin_if_not_exists():
    """Create admin user on first startup if not already present."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM users WHERE username = %s",
                    (ADMIN_USERNAME,),
                )
                if not cur.fetchone():
                    hashed_password = pwd_context.hash(ADMIN_PASSWORD)
                    cur.execute(
                        "INSERT INTO users (username, hashed_password) VALUES (%s, %s)",
                        (ADMIN_USERNAME, hashed_password),
                    )
                    conn.commit()
                    logger.info("Admin user created successfully")
                else:
                    logger.info("Admin user already exists")
    except Exception as e:
        logger.error("Error creating admin user", extra={"error": str(e)})
        raise

# -------------------------------------------------------
# Application lifespan (replaces deprecated @app.on_event)
# -------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Fraud Rule Engine API")
    create_admin_if_not_exists()
    logger.info("Startup complete")
    yield
    # Shutdown: close the connection pool cleanly
    db_pool.closeall()
    logger.info("Database connection pool closed")

# -------------------------------------------------------
# FastAPI app
# -------------------------------------------------------
app = FastAPI(
    title="Fraud Rule Engine API",
    description="API for retrieving flagged fraud transactions",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# -------------------------------------------------------
# Middleware: correlation ID + metrics
# -------------------------------------------------------
@app.middleware("http")
async def observability_middleware(request: Request, call_next):
    # Propagate or generate correlation ID
    correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
    correlation_id_var.set(correlation_id)

    start = time.time()
    response = await call_next(request)
    duration = time.time() - start

    response.headers["X-Correlation-ID"] = correlation_id

    # Record Prometheus metrics
    REQUEST_COUNT.labels(
        method=request.method,
        endpoint=request.url.path,
        status_code=response.status_code,
    ).inc()
    REQUEST_DURATION.labels(
        method=request.method,
        endpoint=request.url.path,
    ).observe(duration)

    return response

# -------------------------------------------------------
# Health endpoints (split liveness from readiness)
# -------------------------------------------------------
@app.get("/health/live", tags=["Health"])
def liveness():
    """
    Liveness probe — confirms the process is running.
    Used by container orchestrators to decide whether to restart the container.
    Returns 200 as long as the process is alive.
    """
    return {"status": "alive", "service": "fraud-rule-engine", "version": "1.0.0"}


@app.get("/health/ready", tags=["Health"])
def readiness():
    """
    Readiness probe — confirms the service can handle traffic.
    Checks the database connection. Returns 503 if the DB is unreachable.
    Used by load balancers to decide whether to route traffic here.
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return {"status": "ready", "database": "connected"}
    except Exception as e:
        logger.error("Readiness check failed", extra={"error": str(e)})
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database unavailable",
        )


@app.get("/metrics", tags=["Observability"])
def metrics():
    """
    Prometheus metrics endpoint.
    Exposes request counts, durations, and DB pool stats for scraping.
    """
    DB_POOL_AVAILABLE.set(len(db_pool._pool))
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


# -------------------------------------------------------
# Authentication
# -------------------------------------------------------
@app.post("/v1/login", response_model=Token, tags=["Authentication"])
def login(request: LoginRequest):
    """Authenticate and return a JWT bearer token."""
    try:
        with get_db_connection() as conn:
            user = authenticate_user(request.username, request.password, conn)

        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect username or password",
                headers={"WWW-Authenticate": "Bearer"},
            )

        access_token = create_access_token(data={"sub": user["username"]})
        return {"access_token": access_token, "token_type": "bearer"}

    except psycopg2.Error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database connection error",
        )


# -------------------------------------------------------
# Flagged transaction endpoints
# -------------------------------------------------------
@app.get("/v1/flagged", response_model=PaginatedFlaggedTransactions, tags=["Transactions"])
def get_flagged_transactions(
    current_user: str = Depends(get_current_user),
    limit: int = 100,
    offset: int = 0,
):
    """
    Return a paginated list of flagged transactions.
    Requires a valid JWT bearer token.
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Total count for pagination metadata
                cur.execute("SELECT COUNT(*) FROM flagged_transactions")
                total = cur.fetchone()["count"]

                cur.execute(
                    """
                    SELECT id, transaction_id, reason, flagged_at, rule_ids
                    FROM flagged_transactions
                    ORDER BY flagged_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    (limit, offset),
                )
                rows = cur.fetchall()

        items = [
            FlaggedTransaction(
                id=row["id"],
                transaction_id=row["transaction_id"],
                reason=row["reason"],
                flagged_at=row["flagged_at"],
                rule_ids=row.get("rule_ids"),
            )
            for row in rows
        ]
        return PaginatedFlaggedTransactions(
            total=total,
            limit=limit,
            offset=offset,
            items=items,
        )

    except psycopg2.Error as e:
        logger.error("Database error in GET /v1/flagged", extra={"error": str(e)})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving transactions",
        )


@app.get("/v1/flagged/{transaction_id}", response_model=FlaggedTransaction, tags=["Transactions"])
def get_flagged_transaction(
    transaction_id: str,
    current_user: str = Depends(get_current_user),
):
    """
    Return a single flagged transaction by transaction ID.
    Requires a valid JWT bearer token.
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, transaction_id, reason, flagged_at, rule_ids
                    FROM flagged_transactions
                    WHERE transaction_id = %s
                    """,
                    (transaction_id,),
                )
                row = cur.fetchone()

        if not row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Transaction not found",
            )

        return FlaggedTransaction(
            id=row["id"],
            transaction_id=row["transaction_id"],
            reason=row["reason"],
            flagged_at=row["flagged_at"],
            rule_ids=row.get("rule_ids"),
        )

    except psycopg2.Error as e:
        logger.error(
            "Database error in GET /v1/flagged/{transaction_id}",
            extra={"error": str(e), "transaction_id": transaction_id},
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving transaction",
        )