# Dockerfile for the generating the Docker image for the Plastron HTTP Daemon (plastrond-http)
#
# To build, from the main plastron project directory, run:
#
# docker build -t docker.lib.umd.edu/plastrond-http:<VERSION> -f plastron-web/Dockerfile .
#
# where <VERSION> is the Docker image version to create.

FROM python:3.8.18-slim
RUN apt-get update && apt-get install -y git

RUN mkdir -p /opt/plastron
COPY plastron-client /opt/plastron/plastron-client
COPY plastron-models /opt/plastron/plastron-models
COPY plastron-rdf /opt/plastron/plastron-rdf
COPY plastron-repo /opt/plastron/plastron-repo
COPY plastron-utils /opt/plastron/plastron-utils
COPY plastron-web /opt/plastron/plastron-web
COPY plastron-cli /opt/plastron/plastron-cli
COPY plastron-stomp /opt/plastron/plastron-stomp

WORKDIR /opt/plastron
RUN pip install \
    './plastron-utils' \
    './plastron-client' \
    './plastron-rdf' \
    './plastron-models' \
    './plastron-repo' \
    './plastron-web' \
    './plastron-cli' \
    './plastron-stomp'

ENV PYTHONUNBUFFERED=1
VOLUME /var/opt/plastron/msg
VOLUME /var/opt/plastron/jobs

EXPOSE 5000

ENTRYPOINT ["plastrond-http", "-c", "/etc/plastron-config.yml"]
