"""Compatibility import for the previous SQS listener module name."""

from sentence_similarity.sqs_processor import SqsProcessor

SqsListener = SqsProcessor

__all__ = ["SqsListener", "SqsProcessor"]
