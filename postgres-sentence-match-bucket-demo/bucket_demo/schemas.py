from pydantic import BaseModel, ConfigDict


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
    sourceGlobalId: int
    sourceTspId: int
    sourceANumber: int
    matches: list

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "sourceGlobalId": 1,
                "sourceTspId": 1001,
                "sourceANumber": 90001,
                "matches": [
                    {
                        "global_id": 10,
                        "distance": 0.0,
                        "tsp_id": 1010,
                        "a_number": 90010,
                    },
                    {
                        "global_id": 40,
                        "distance": 0.20,
                        "tsp_id": 1040,
                        "a_number": 90040,
                    },
                ],
            }
        }
    )
