FROM python:3.10

ENV LANG C.UTF-8

WORKDIR /app

ARG CONSUL_HTTP_TOKEN
ENV APP_ENV=${ENV}
ARG CONSUL_HTTP_ADDR=${CONSUL_HTTP_ADDR}
ARG CONSUL_HTTP_TOKEN=${CONSUL_HTTP_TOKEN}
ARG CONSUL_TEMPLATE_VERSION=0.25.1
RUN wget "https://releases.hashicorp.com/consul-template/${CONSUL_TEMPLATE_VERSION}/consul-template_${CONSUL_TEMPLATE_VERSION}_linux_amd64.tgz"
RUN tar zxfv consul-template_${CONSUL_TEMPLATE_VERSION}_linux_amd64.tgz
RUN docker/entrypoint.sh
RUN rm -f consul-template_${CONSUL_TEMPLATE_VERSION}_linux_amd64.tgz
RUN rm -f consul-template

COPY requirements.txt /app
RUN pip install -r requirements.txt

COPY . /app

CMD ["python", "-m", "src.main"]
