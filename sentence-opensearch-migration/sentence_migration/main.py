from fastapi import FastAPI

from sentence_migration.routes import router

app = FastAPI(
    title="Sentence OpenSearch Migration",
    version="1.0.0",
    description=(
        "Migrates S3-listed TSP documents from a paginated sentence API to existing "
        "OpenSearch occurrence and semantic-catalog aliases. OpenSearch is never "
        "contacted during application startup."
    ),
)
app.include_router(router)
