FROM node:14-alpine

WORKDIR /app
RUN apk add --update git ca-certificates
COPY package.json .
COPY yarn.lock .
RUN yarn install
COPY . .
RUN yarn run build:prod

FROM gcr.io/shopify-base-images/alpine:latest
WORKDIR /bq-grafana
COPY --from=0 /app/dist .
