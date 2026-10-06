from fastapi import FastAPI

from bucket_demo.routes import router

app = FastAPI(
    title="PostgreSQL Sentence Match POC",
    version="1.0.0",
    description=(
        "Test bidirectional JSONB writes owned by PostgreSQL functions."
    ),
)

app.include_router(router)
