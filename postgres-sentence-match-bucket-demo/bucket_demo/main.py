from fastapi import FastAPI

from bucket_demo.routes import router


app = FastAPI(
    title="PostgreSQL Sentence Match Bucket Demo",
    version="1.0.0",
    description=(
        "Generate a fake sentence-key request and compare bidirectional "
        "bucket inserts through a PostgreSQL function or Python SQLAlchemy."
    ),
)

app.include_router(router)
