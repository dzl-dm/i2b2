#!/usr/bin/env bash
## Import the secrets as env vars - when not using secrets, this is not run
file_env() {
  local var="$1"
  local fileVar="${var}_FILE"
  local def="${2:-}"

  if ! test -f ${!fileVar} ; then
    echo >&2 "warning: '${!fileVar}' not available in this container"
    return 1
  fi
  if [ "${!var:-}" ] && [ "${!fileVar:-}" ]; then
    echo >&2 "error: both $var and $fileVar are set (but are exclusive)"
    exit 1
  fi
  local val="$def"
  if [ "${!var:-}" ]; then
    val="${!var}"
  elif [ "${!fileVar:-}" ]; then
    val="$(< "${!fileVar}")"
  fi
  export "$var"="$val"
  ## Put the variables into the profile so a new shell will also have them
  sed -i "/export ${var}=/d" /etc/profile.d/docker.sh
  echo -e "export ${var}=${val}" >> /etc/profile.d/docker.sh
  unset "$fileVar"
}

echo -e "Processing secrets..."
if test -f /run/secrets/ENV_MULTILOAD; then
    echo >&2 -e "Loading ENV_MULTILOAD secrets..."
    source /run/secrets/ENV_MULTILOAD
fi
while read secret_env; do
  secret_var=$(echo ${secret_env} | cut -d= -f 1)
  file_env ${secret_var:0:-5};
done < <(env | grep '_FILE=')
echo -e "...complete"

## Map upstream ENV vars
export APP_ID=${WEB_FQDN}

## Customisation
echo >&2 "Setting custom settings..."
cd /var/www/html/
find  . -maxdepth 2 -type f -name i2b2_config_data.js -exec sed -i "s*name: \"HarvardDemo\",*name: \"${ORGANISATION_NAME}\",*g" {} \;
sed -i 's/allowAnalysis: true,/allowAnalysis: true,\n\t\t  adminOnly: true,/g' admin/i2b2_config_data.js
sed -i "s/#ServerName www.example.com:80/#ServerName www.example.com:80\nServerName ${WEB_FQDN}/g" /etc/httpd/conf/httpd.conf
if [ ${INCLUDE_DEMO_DATA} != "True" ]; then
    find  . -maxdepth 3 -type f -name i2b2_ui_config.js -exec sed -i 's/loginDefaultUsername : "demo"/loginDefaultUsername : ""/g' {} \; -exec sed -i 's/loginDefaultPassword : "demouser"/loginDefaultPassword : ""/g' {} \;
fi
if timeout 15 ping -c 2 i2b2-api ; then
    echo >&2 "api server is available, adding proxy to apache config"
    cp /docker-entrypoint/conf/i2b2-api.conf /etc/httpd/conf.d/
fi
cd -

## Now call the original/upstream entrypoint and append arguments
echo -e "Starting apache web server..."
/bin/sh /run-httpd.sh
# exec "$@"
