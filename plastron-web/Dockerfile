# Dockerfile for the generating the Docker image for the Plastron HTTP Daemon (plastrond-http)
#
# To build, from the main plastron project directory, run:
#
# docker build -t docker.lib.umd.edu/plastrond-http:<VERSION> -f plastron-web/Dockerfile .
#
# where <VERSION> is the Docker image version to create.

FROM python:3.10.18-slim

RUN mkdir -p /opt/plastron
COPY VERSION /opt/plastron/VERSION

WORKDIR /opt/plastron

# interleave the copy and pip install commands for better Docker build caching
COPY plastron-utils /opt/plastron/plastron-utils
RUN pip install './plastron-utils'

COPY plastron-client /opt/plastron/plastron-client
RUN pip install './plastron-client'

COPY plastron-rdf /opt/plastron/plastron-rdf
RUN pip install './plastron-rdf'

COPY plastron-messaging /opt/plastron/plastron-messaging
RUN pip install './plastron-messaging'

COPY plastron-models /opt/plastron/plastron-models
RUN pip install './plastron-models'

COPY plastron-repo /opt/plastron/plastron-repo
RUN pip install './plastron-repo'

COPY plastron-jobs /opt/plastron/plastron-jobs
RUN pip install './plastron-jobs'

COPY plastron-web /opt/plastron/plastron-web
RUN pip install './plastron-web'

ENV PYTHONUNBUFFERED=1
VOLUME /var/opt/plastron/msg
VOLUME /var/opt/plastron/jobs

EXPOSE 5000

ENTRYPOINT ["plastrond-http", "-c", "/etc/plastron-config.yml"]
