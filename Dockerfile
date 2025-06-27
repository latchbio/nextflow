# syntax = docker/dockerfile:1.4.1

from alpine:3.22.0

run apk add \
    bash \
    s5cmd \
    openjdk21-jre-headless

copy ./.nextflow /root/.nextflow
copy ./nextflow /usr/bin/nextflow
