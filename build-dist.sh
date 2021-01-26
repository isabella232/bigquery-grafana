#!/usr/bin/env bash

rm bq-plugin.zip

set -e

env GOOS=linux GOARCH=amd64 go build -o dist/doitintl-bigquery-datasource-linux-amd64 ./pkg/...
yarn run build:prod
zip -r bq-plugin.zip dist/
