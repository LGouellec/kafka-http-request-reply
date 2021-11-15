#!/bin/bash

echo "Creating http-sink connector"
curl -X PUT \
     -H "Content-Type: application/json" \
     --data '{
              "topics": "http-messages",
               "tasks.max": "1",
               "max.retries": "1",
               "behavior.on.error": "log",
               "connector.class": "io.confluent.connect.http.HttpSinkConnector",
               "key.converter": "org.apache.kafka.connect.storage.StringConverter",
               "value.converter": "org.apache.kafka.connect.storage.StringConverter",
               "confluent.topic.bootstrap.servers": "kafka:29092",
               "confluent.topic.replication.factor": "1",
               "reporter.bootstrap.servers": "kafka:29092",
               "reporter.error.topic.name": "error-responses",
               "reporter.error.topic.replication.factor": 1,
               "reporter.errors.as": "error_string",
               "reporter.result.topic.name": "success-responses",
               "reporter.result.topic.replication.factor": 1,
               "reporter.result.topic.key.format": "string",
               "reporter.error.topic.key.format": "string",
               "reporter.result.topic.value.format": "string",
               "reporter.error.topic.value.format": "string",
               "http.api.url": "http://web-service:8080/api/messages",
               "request.method": "POST"
          }' \
     http://localhost:8083/connectors/http-sink/config | jq .