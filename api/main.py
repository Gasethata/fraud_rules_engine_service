from fastapi import FastAPI, Depends, HTTPException, status
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from typing import List
from contextlib import contextmanager

from schemas import Token, LoginRequest, FlaggedTransaction
from auth import authenticate_user, create_access_token, get_current_user
from passlib.context import CryptContext


app = FastAPI(
    title="Fraud Rule Engine API",
    description="API for retrieving flagged transactions",
    version="1.0.0",
    docs_url="/docs",  # Explicit docs URL
    redoc_url="/redoc",  # Explicit redoc URL
)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Validate required environment variables at startup
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")

if not ADMIN_USERNAME or not ADMIN_PASSWORD:
    raise ValueError("ADMIN_USERNAME and ADMIN_PASSWORD must be set")

# Database configuration
db_config = {
    "host": os.getenv("HOST"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "cursor_factory": RealDictCursor,  # Return dicts for easier handling
}

# Validate database config
for key, value in db_config.items():
    if key != "port" and value is None:  # port has default
        raise ValueError(f"Database configuration {key} is not set")


@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        yield conn
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        raise
    finally:
        if conn:
            conn.close()


def create_admin_if_not_exists():
    """Create admin user if it doesn't exist"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Check if admin exists
                cur.execute(
                    "SELECT id FROM users WHERE username = %s",
                    (ADMIN_USERNAME,)
                )
                user_exists = cur.fetchone()

                if not user_exists:
                    hashed_password = pwd_context.hash(ADMIN_PASSWORD)
                    cur.execute(
                        "INSERT INTO users (username, hashed_password) VALUES (%s, %s)",
                        (ADMIN_USERNAME, hashed_password),
                    )
                    conn.commit()
                    print("Admin user created successfully.")
                else:
                    print("Admin user already exists.")
    except Exception as e:
        print(f"Error creating admin user: {e}")
        # Don't raise - allow app to start even if admin creation fails
        # The endpoint will still work for existing users


@app.on_event("startup")
def startup():
    """Initialize application on startup"""
    print("Starting Fraud Rule Engine API...")
    create_admin_if_not_exists()
    print("Startup complete")


@app.get("/health", tags=["Health"])
def health():
    """Health check endpoint"""
    return {
        "status": "ok",
        "service": "fraud-rule-engine",
        "version": "1.0.0"
    }


@app.post("/login", response_model=Token, tags=["Authentication"])
def login(request: LoginRequest):
    """Authenticate user and return JWT token"""
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
            detail="Database connection error"
        )


@app.get("/flagged", response_model=List[FlaggedTransaction], tags=["Transactions"])
def get_flagged_transactions(current_user: str = Depends(get_current_user), limit: int = 100, offset: int = 0):
    """Get all flagged transactions (authenticated endpoint)"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, transaction_id, reason, flagged_at, rule_ids
                    FROM flagged_transactions
                    ORDER BY flagged_at DESC
                    LIMIT %s OFFSET %s
                """, (limit, offset))

                rows = cur.fetchall()

                # Convert to response model
                return [
                    FlaggedTransaction(
                        id=row["id"],
                        transaction_id=row["transaction_id"],
                        reason=row["reason"],
                        flagged_at=row["flagged_at"],
                        rule_ids=row.get("rule_ids")  # Safe access if column exists
                    )
                    for row in rows
                ]

    except psycopg2.Error as e:
        print(f"Database error in /flagged: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving transactions"
        )


# Optional: Add a specific endpoint for a single transaction
@app.get("/flagged/{transaction_id}", response_model=FlaggedTransaction, tags=["Transactions"])
def get_flagged_transaction(transaction_id: str, current_user: str = Depends(get_current_user)):
    """Get a specific flagged transaction by ID"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, transaction_id, reason, flagged_at, rule_ids
                    FROM flagged_transactions
                    WHERE transaction_id = %s
                """, (transaction_id,))

                row = cur.fetchone()

                if not row:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="Transaction not found"
                    )

                return FlaggedTransaction(
                    id=row["id"],
                    transaction_id=row["transaction_id"],
                    reason=row["reason"],
                    flagged_at=row["flagged_at"],
                    rule_ids=row.get("rule_ids")
                )

    except psycopg2.Error as e:
        print(f"Database error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving transaction"
        )