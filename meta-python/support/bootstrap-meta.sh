#!/usr/bin/env bash
## Import the secrets as env vars
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
    echo -e "Loading ENV_MULTILOAD secrets..."
    source /run/secrets/ENV_MULTILOAD
fi
while read secret_env; do
  secret_var=$(echo ${secret_env} | cut -d= -f 1)
  file_env ${secret_var:0:-5};
done < <(env | grep '_FILE=')
echo -e "...complete"

echo -e "Setting config files from env vars..."
source /conf.cfg
sed -i "s#^\(generator\.sparql_endpoint\s*=\s*\).*\$#\1${generator_sparql_endpoint}#" /ontology.properties
sed -i "s#^\(generator\.output_dir\s*=\s*\).*\$#\1${TEMPDIR}/i2b2-sql/#" /ontology.properties
mkdir -p "${TEMPDIR}/i2b2-sql/"

## Now call the original/upstream entrypoint and append arguments
echo -e "Starting api server..."
