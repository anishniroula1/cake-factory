from pydantic import BaseModel
from pydantic import ConfigDict


class InitRequest(BaseModel):
    analysisGroup: str = "Asylee"
    matchCount: int = 10
    randomSeed: int = 42
    clearExisting: bool = True

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "analysisGroup": "Asylee",
                "matchCount": 10,
                "randomSeed": 42,
                "clearExisting": True,
            }
        }
    )


class MatchRequest(BaseModel):
    analysisGroup: str = "Asylee"
    sourceSentenceKey: str
    matches: dict

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "analysisGroup": "Asylee",
                "sourceSentenceKey": (
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                ),
                "matches": {
                    (
                        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    ): 0.10,
                    (
                        "cccccccccccccccccccccccccccccccc"
                        "cccccccccccccccccccccccccccccccc"
                    ): 0.20,
                },
            }
        }
    )
