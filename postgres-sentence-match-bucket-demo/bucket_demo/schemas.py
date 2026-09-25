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


class ListMatchRequest(BaseModel):
    analysisGroup: str = "Asylee"
    sourceGlobalId: int
    sourceTspId: int
    sourceApplicationId: str
    matches: list

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "analysisGroup": "Asylee",
                "sourceGlobalId": 1,
                "sourceTspId": 1001,
                "sourceApplicationId": "APP-1",
                "matches": [
                    {
                        "global_id": 10,
                        "distance": 0.10,
                        "tsp_id": 1010,
                        "application_id": "APP-10",
                    },
                    {
                        "global_id": 40,
                        "distance": 0.20,
                        "tsp_id": 1040,
                        "application_id": "APP-40",
                    },
                ],
            }
        }
    )
