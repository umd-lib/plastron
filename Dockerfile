# Dockerfile for the generating the Plastron Docker image
#
# To build:
#
# docker build -t docker.lib.umd.edu/plastrond:<VERSION> -f Dockerfile .
#
# where <VERSION> is the Docker image version to create.
FROM python:3.7.15-slim

RUN mkdir -p /opt/plastron
COPY . /opt/plastron
WORKDIR /opt/plastron
RUN pip install -r requirements.txt .
ENV PYTHONUNBUFFERED=1
VOLUME /var/opt/plastron/msg
VOLUME /var/opt/plastron/jobs

EXPOSE 5000

ENTRYPOINT ["plastrond", "-c", "/etc/plastrond.yml"]
