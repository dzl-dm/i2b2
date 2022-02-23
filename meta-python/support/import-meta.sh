#!/bin/sh
## Run all the parts which make the meta-data translation and import from CoMetaR (fuseki) to i2b2

## Run plan from post-receive git hook
conffile=/conf.cfg
while [ ! $# -eq 0 ]
do
	case "$1" in
		-p)
			shift
			conffile=$(realpath "$1")
			;;
	esac
	shift
done

echo "Reading conf file: $conffile"
. "$conffile"
touch "$LOGFILE"

echo "$(date +'%d.%m.%y %H:%M:%S') ---------------- CoMetaR Translation ---------------------" | tee -a "$LOGFILE"

echo "$(date +'%d.%m.%y %H:%M:%S') i2b2 import sql is being produced." | tee -a "$LOGFILE"
mkdir -p "$TEMPDIR/i2b2-sql"
rm -f "$TEMPDIR/i2b2-sql/*.sql"
2>&1 java -cp dependency/\* de.dzl.cometar.SQLFileWriter ontology.properties | tee -a "$LOGFILE"
# rm "$TEMPDIR/export.ttl"

echo "$(date +'%d.%m.%y %H:%M:%S') Metadata import into i2b2 server (part 1/2)..." | tee -a "$LOGFILE"
2>&1 PGPASSWORD=$I2B2METAPW /usr/bin/psql -v ON_ERROR_STOP=1 -v statement_timeout=120000 -L "$TEMPDIR/postgres.log" -q --host=$I2B2DBHOST --username=$I2B2METAUSER --dbname=$I2B2DBNAME -f "$TEMPDIR/i2b2-sql/meta.sql" | tee -a "$LOGFILE"
if [ $? -eq 1 ] || [ $? -eq 2 ] || [ $? -eq 3 ]; then
	echo "PostgreSQL command failed." | tee -a "$LOGFILE"
	## TODO: Is the request form standard? Get URL from config
	# curl -X POST https://data.dzl.de/biomaterial_request/sendform.php -H "Content-Type: application/x-www-form-urlencoded" -d "formtype=postgresql_fail&log=$(cat '$TEMPDIR/postgres.log')"
fi
echo "$(date +'%d.%m.%y %H:%M:%S') Metadata import into i2b2 server (part 2/2)..." | tee -a "$LOGFILE"
2>&1 PGPASSWORD=$I2B2DEMOPW /usr/bin/psql -v ON_ERROR_STOP=1 -v statement_timeout=120000 -L "$TEMPDIR/postgres.log" -q --host=$I2B2DBHOST --username=$I2B2DEMOUSER --dbname=$I2B2DBNAME -f "$TEMPDIR/i2b2-sql/data.sql" | tee -a "$LOGFILE"
if [ $? -eq 1 ] || [ $? -eq 2 ] || [ $? -eq 3 ]; then
	echo "PostgreSQL command failed." | tee -a "$LOGFILE"
	# curl -X POST https://data.dzl.de/biomaterial_request/sendform.php -H "Content-Type: application/x-www-form-urlencoded" -d "formtype=postgresql_fail&log=$(cat '$TEMPDIR/postgres.log')"
fi
echo "$(date +'%d.%m.%y %H:%M:%S') Refreshing patient count..." | tee -a "$LOGFILE"
2>&1 PGPASSWORD=$DB_ADMIN_PASS /usr/bin/psql -v ON_ERROR_STOP=1 -v statement_timeout=120000 -L "$TEMPDIR/postgres.log" -q --host=$I2B2DBHOST --username=$DB_ADMIN_USER --dbname=$I2B2DBNAME -f "/patient_count.sql" | tee -a "$LOGFILE"
if [ $? -eq 1 ] || [ $? -eq 2 ] || [ $? -eq 3 ]; then
	echo "PostgreSQL command failed." | tee -a "$LOGFILE"
	# curl -X POST https://data.dzl.de/biomaterial_request/sendform.php -H "Content-Type: application/x-www-form-urlencoded" -d "formtype=postgresql_fail&log=$(cat '$TEMPDIR/postgres.log')"
fi

echo ------------------------------------- | tee -a "$LOGFILE"

