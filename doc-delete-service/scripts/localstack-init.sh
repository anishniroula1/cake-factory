#!/bin/sh
set -eu

awslocal sqs create-queue \
  --queue-name document-delete-local.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=true

awslocal sqs create-queue \
  --queue-name case-similarity-local.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=true

