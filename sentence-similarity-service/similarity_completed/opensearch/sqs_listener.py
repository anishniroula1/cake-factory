"""Compatibility import for the previous SQS listener module name."""

from similarity_completed.opensearch.sqs_processor import SqsProcessor

SqsListener = SqsProcessor

__all__ = ["SqsListener", "SqsProcessor"]
