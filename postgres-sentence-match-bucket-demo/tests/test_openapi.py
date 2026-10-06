from bucket_demo.main import app


def testSwaggerContainsOnlySqlFunctionCalls():
    """Keep the small SQL-function-only POC visible."""

    paths = set(app.openapi()["paths"])

    assert paths == {
        "/health",
        "/init",
        "/matches/sql-function",
        "/list-matches/sql-function",
    }
