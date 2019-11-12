FROM python:3.6.2-slim

RUN mkdir -p /opt/plastron
COPY . /opt/plastron
WORKDIR /opt/plastron
RUN pip install .
ENV PYTHONUNBUFFERED=1
VOLUME /var/opt/plastron

CMD ["plastrond", "-c", "/etc/plastrond.yml"]