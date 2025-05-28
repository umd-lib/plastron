# Dockerfile for the generating the Docker image for the Plastron STOMP Daemon (plastrond-stomp)
#
# To build, from the main plastron project directory, run:
#
# docker build -t docker.lib.umd.edu/plastrond-stomp:<VERSION> -f plastron-stomp/Dockerfile .
#
# where <VERSION> is the Docker image version to create.

FROM python:3.8.18-slim

RUN mkdir -p /opt/plastron
COPY VERSION /opt/plastron/VERSION
COPY plastron-client /opt/plastron/plastron-client
COPY plastron-models /opt/plastron/plastron-models
COPY plastron-rdf /opt/plastron/plastron-rdf
COPY plastron-messaging /opt/plastron/plastron-messaging
COPY plastron-repo /opt/plastron/plastron-repo
COPY plastron-jobs /opt/plastron/plastron-jobs
COPY plastron-stomp /opt/plastron/plastron-stomp
COPY plastron-utils /opt/plastron/plastron-utils

WORKDIR /opt/plastron
RUN pip install \
    './plastron-utils' \
    './plastron-client' \
    './plastron-rdf' \
    './plastron-messaging' \
    './plastron-models' \
    './plastron-repo' \
    './plastron-jobs' \
    './plastron-stomp'

ENV PYTHONUNBUFFERED=1
VOLUME /var/opt/plastron/msg
VOLUME /var/opt/plastron/jobs

ENTRYPOINT ["plastrond-stomp", "-c", "/etc/plastrond.yml", "-v"]
