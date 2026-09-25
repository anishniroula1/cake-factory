from bucket_demo.main import app


def testSwaggerOnlyContainsTheFourRequestedCalls():
    """Keep the demo API small."""

    paths = set(app.openapi()["paths"])

    assert paths == {
        "/health",
        "/init",
        "/matches/sql-function",
        "/matches/python",
    }
