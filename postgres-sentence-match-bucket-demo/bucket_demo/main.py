from fastapi import FastAPI

from bucket_demo.routes import router


app = FastAPI(
    title="PostgreSQL Sentence Match POC",
    version="1.0.0",
    description=(
        "Compare bidirectional JSONB bucket and JSONB list inserts through "
        "PostgreSQL functions or Python SQLAlchemy."
    ),
)

app.include_router(router)
