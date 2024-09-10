#!/bin/sh

set -e
echo "Validating required params..."

if [ -z $APP_ENV ]; then
  echo "APP_ENV is missing"
  exit 1
fi

if [ -z $CONSUL_HTTP_ADDR ]; then
  echo "CONSUL_HTTP_ADDR is missing"
  exit 2
fi

if [ -z $CONSUL_HTTP_TOKEN ]; then
  echo "CONSUL_HTTP_TOKEN is missing"
  exit 3
fi

# dynamicaly parse a Consul template
sed -e "s/{APP_ENV}/$APP_ENV/g" ./docker/consul/env.tmpl > ./env.tmpl
./consul-template -template="./env.tmpl:./.env" \
                 -config ./docker/consul/config.hcl -once -log-level info

# Star app
# npm start