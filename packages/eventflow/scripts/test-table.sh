#!/bin/sh
# The table's contract, on DynamoDB Local: EVENTFLOW_TEST_ENDPOINT, or a container that
# this starts and stops. The memory store alone doesn't have the races.
set -e
cd "$(dirname "$0")/.."
if [ -z "$EVENTFLOW_TEST_ENDPOINT" ]; then
  id=$(docker run -d --rm -p 0:8000 amazon/dynamodb-local)
  trap 'docker stop "$id" > /dev/null' EXIT
  EVENTFLOW_TEST_ENDPOINT="http://localhost:$(docker port "$id" 8000/tcp | head -n 1 | sed 's/.*://')"
fi
export EVENTFLOW_TEST_ENDPOINT
export EVENTFLOW_TEST_TABLE="eventflow-test"
for i in $(seq 1 30); do
  curl -s -o /dev/null "$EVENTFLOW_TEST_ENDPOINT" && break
  sleep 1
done
curl -s -o /dev/null "$EVENTFLOW_TEST_ENDPOINT" \
  -H 'Content-Type: application/x-amz-json-1.0' \
  -H 'X-Amz-Target: DynamoDB_20120810.CreateTable' \
  -H 'Authorization: AWS4-HMAC-SHA256 Credential=local/20260101/local/dynamodb/aws4_request, SignedHeaders=host, Signature=0' \
  -d '{"TableName":"eventflow-test","BillingMode":"PAY_PER_REQUEST","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}]}'
AWS_REGION="${AWS_REGION:-local}" bun test --timeout 120000 test/store-contract.test.ts
