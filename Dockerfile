# syntax = docker/dockerfile:1.4.1

from alpine:3.22.0

run apk add \
    bash \
    curl \
    openjdk21-jre-headless

run curl -sSL https://github.com/jqlang/jq/releases/download/jq-1.8.1/jq-linux-amd64 -o /bin/jq
run chmod +x /bin/jq

copy ./.nextflow /root/.nextflow
copy ./nextflow /usr/bin/nextflow
