from bucket_demo.main import app


def testSwaggerContainsTheRequestedCalls():
    """Keep both bucket and JSONB-list comparisons visible."""

    paths = set(app.openapi()["paths"])

    assert paths == {
        "/health",
        "/init",
        "/matches/sql-function",
        "/matches/python",
        "/list-matches/sql-function",
        "/list-matches/python",
    }
