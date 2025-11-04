# /app/main.py
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
# from fastapi.middleware.trustedhost import TrustedHostMiddleware
# from fastapi.middleware.gzip import GZipMiddleware
from contextlib import asynccontextmanager
import logging
import signal
import uvicorn
from app.config.settings import config
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.api.routes import router
from app.services.stream_service import stream_manager, initialize_stream_manager

from datetime import datetime, timezone
from app.services.database import (
    init_db_pool, close_db_pool, connection_pool, 
    initialize_database, ensure_database_indices 
)

# Setup logging
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(config.get("log_file_path", "app.log")),
        logging.StreamHandler()
    ])
logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()

async def cleanup_expired_data():
    """Clean up expired records from various tables."""
    logger.info("Executing scheduled job: cleanup_expired_data")
    try:
        if not connection_pool or connection_pool._closed:
            logger.warning("cleanup_expired_data: DB pool not available or closed.")
            return

        async with connection_pool.acquire() as conn:
            async with conn.transaction():
                # Clean up expired OTPs
                await conn.execute("DELETE FROM otps WHERE expires_at < CURRENT_TIMESTAMP")
                logger.info("Expired OTPs cleaned up.")

                # Clean up expired sessions
                await conn.execute("DELETE FROM sessions WHERE expires_at < CURRENT_TIMESTAMP")
                logger.info("Expired sessions cleaned up.")

                # Clean up expired blacklisted tokens
                await conn.execute("DELETE FROM token_blacklist WHERE expires_at < CURRENT_TIMESTAMP")
                logger.info("Expired blacklisted tokens cleaned up.")
    except Exception as e:
        logger.error(f"Error during cleanup_expired_data: {e}", exc_info=True)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Application startup sequence initiated (async)...")

    try:
        await init_db_pool()
        logger.info("Asyncpg database connection pool initialization requested.")
    except Exception as e:
        logger.critical(f"CRITICAL: Failed to initialize asyncpg pool: {e}", exc_info=True)
        raise RuntimeError("Database pool initialization failed.") from e

    try:
        logger.info("Attempting to initialize database schema (async)...")
        if await initialize_database():
            logger.info("Database schema initialized/verified successfully (async).")
    except Exception as e:
        logger.critical(f"CRITICAL: Database schema initialization failed (async): {e}", exc_info=True)
        raise RuntimeError(f"Database schema initialization failed (async): {e}") from e

    try:
        # await ensure_database_indices()
        logger.info("Database indices ensured (async).")
    except Exception as e:
        logger.error(f"Failed to ensure database indices (async): {e}", exc_info=True)

    try:
        await initialize_stream_manager() 
        logger.info("Stream manager initialized and background tasks started.")
    except Exception as e:
        logger.error(f"Failed to initialize StreamManager: {e}", exc_info=True)

    try:
        scheduler.add_job(
            cleanup_expired_data,
            trigger=CronTrigger(hour="*/1"),  # Run every hour
            id="cleanup_expired_data_job", # Ensure unique ID
            replace_existing=True
        )
        scheduler.start()
        logger.info("APScheduler started with cleanup_expired_data scheduled.")
    except Exception as e:
        logger.error(f"Failed to start APScheduler: {e}", exc_info=True)

    logger.info("Application startup complete (async).")
    yield
    # Shutdown
    logger.info("Application shutdown sequence initiated (async)...")

    # APScheduler shutdown
    if scheduler.running:
        try:
            scheduler.shutdown()
            logger.info("APScheduler shut down successfully.")
        except Exception as e:
            logger.error(f"Error during APScheduler shutdown: {e}", exc_info=True)

    if stream_manager:
        try:
            await stream_manager.shutdown() # Already async
            logger.info("Stream manager shutdown complete.")
        except Exception as e:
            logger.error(f"Error during StreamManager shutdown: {e}", exc_info=True)

    if connection_pool and not connection_pool._closed: # Check if pool exists and not closed
        try:
            await close_db_pool()
            logger.info("Asyncpg database connection pool closed.")
        except Exception as e:
            logger.error(f"Error closing asyncpg database connection pool: {e}", exc_info=True)

    logger.info("Application shutdown complete (async).")

from starlette.middleware.base import BaseHTTPMiddleware
class CacheControlMiddleware(BaseHTTPMiddleware): 
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        static_path = config.get("static_path_prefix", "/static/")
        api_prefix = config.get("api_path_prefix", "/api/")
        auth_prefix = config.get("auth_path_prefix", "/auth/")

        if request.url.path.startswith(static_path):
            response.headers['Cache-Control'] = config.get("static_cache_control", "public, max-age=604800")
        elif request.url.path.startswith(api_prefix) or request.url.path.startswith(auth_prefix):
            response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, proxy-revalidate'
        return response

app = FastAPI(
    root_path=config.get("fastapi_root_path", "/insighteye"),
    lifespan=lifespan,
    title=config.get("fastapi_title", "InsightEye API"),
    description=config.get("fastapi_description", "API for managing cameras, users, and insights."),
    version=config.get("fastapi_version", "1.0.0"),
    openapi_url=f"{config.get('fastapi_docs_prefix', '')}/openapi.json",
    docs_url=f"{config.get('fastapi_docs_prefix', '')}/docs",
    redoc_url=f"{config.get('fastapi_docs_prefix', '')}/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Matches main.py's `allow_origins=["*"],#origins`
    allow_credentials=config.get("cors_allow_credentials", True),
    allow_methods=config.get("cors_allow_methods", ["*"]),
    allow_headers=config.get("cors_allow_headers", ["*"]),
    expose_headers=config.get("cors_expose_headers", ["X-Request-ID"])
)

# # Add trusted hosts middleware - Keep commented to match main.py
# app.add_middleware(
#     TrustedHostMiddleware,
#     allowed_hosts=config.get("trusted_hosts", ["localhost", "127.0.0.1"])
# )

@app.middleware("http")
async def add_security_headers(request: Request, call_next): # Identical to main.py
    response = await call_next(request)
    is_secure_scheme = request.url.scheme == "https"
    x_forwarded_proto = request.headers.get("x-forwarded-proto")
    is_behind_secure_proxy = x_forwarded_proto == "https"

    if is_secure_scheme or is_behind_secure_proxy or config.get("force_hsts", False):
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains; preload"

    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"

    if "server" in response.headers:
        del response.headers["server"]
    return response

app.include_router(router)

def signal_handler(signum, frame):
    logger.info(f"Signal {signal.Signals(signum).name} received. Initiating graceful shutdown via Uvicorn.")

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

@app.get("/health", tags=["System"])
async def health_check(): 
    """API health check endpoint"""
    db_healthy = False
    conn_info = "DB pool not checked or inactive"

    if connection_pool and not connection_pool._closed:
        try:
            async with connection_pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            db_healthy = True
            conn_info = (f"Asyncpg DB pool healthy (size: {connection_pool.get_size()}, "
                         f"idle: {connection_pool.get_idle_size()}/{connection_pool.get_max_size()})")
        except Exception as e:
            conn_info = f"Asyncpg DB pool error: {str(e)}"
            logger.warning(f"Health check: Asyncpg DB connection failed - {e}", exc_info=True)
    elif connection_pool and connection_pool._closed:
        conn_info = "Asyncpg DB pool is closed."
    else:
        conn_info = "Asyncpg DB pool not initialized."

    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "database_status": conn_info,
        "db_connection_ok": db_healthy
    }

if __name__ == "__main__":
    server_host = config.get("server_host", "0.0.0.0")
    server_port = config.get("server_port", 8001)
    reload_app = config.get("debug_reload", False) 

    logger.info(f"Starting Uvicorn server on {server_host}:{server_port} with reload: {reload_app}")
    uvicorn.run(
        "main:app", 
        host=server_host,
        port=server_port,
    )
    