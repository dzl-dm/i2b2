
# Introduction

[i2b2](https://www.i2b2.org/) is a community maintained project designed for "Informatics for Integrating Biology and the Bedside"

They provide the software capability, this project is to ease deployment. The defaults settings will provide a deployment with some demographics data pre-installed, which is part of i2b2's demo data. You can optionally add all of the demo data with a setting in the environments file.

We aim to provide a complete Data Warehouse solution for the medical informatics domain, so we also provide notes on how to deploy with our [dwh-https-proxy](https://github.com/dzl-dm/dwh-https-proxy) - see also our [parent github page](https://github.com/dzl-dm/). This i2b2 system can also be deployed in different environments.

# Pre-requisites
We are assuming a clean, linux-based environment (git-bash and other windows based mechanisms should also work) with docker already installed and available to the user you are logged in as.
Tested docker version: 20.10.11

# Basic usage
We provide two options for deployment:
* [Streamlined deployment](#streamlined-deployment) - Which is a purely environment variable based configuration mechanism. This includes setting and passing passwords as envoronment variables from your host to the docker containers
*  [Advanced 'secret' deployment](#advanced-secret-deployment-alternative) - Which has some security benefits so the secrets cannot be seen from the docker host system. The environment based system still does not expose passwords outside of your server environment, unless you have explicitly configured it to do so.

## Streamlined deployment
The first step is to copy the essential files from our repo as templates, these will then be edited for you environment (you may prefer to prepare everything on your desktop then copy over to the deployment system)
```sh
wget https://raw.githubusercontent.com/dzl-dm/i2b2/master/docker-compose.yml
wget https://raw.githubusercontent.com/dzl-dm/i2b2/master/.env
wget https://raw.githubusercontent.com/dzl-dm/i2b2/master/.env-secrets
```
> _NOTE:_ If you prefer to build the images yourself (or edit the way they are built), you could clone our whole repo: `git clone https://github.com/dzl-dm/i2b2.git`

Out of the box, this should deploy a functional installation of i2b2, simply by running this in the directory with your `docker-compose.yml` and `.env*` files:
```sh
docker-compose up -d
```

We strongly recommend changing the passwords stored in `.env-secrets` before deployment. Additionally, you must [change the user passwords](#change-default-passwords) once the application is running 

## Advanced 'secret' deployment (alternative)
The first step is to copy the essential files from our repo as templates, these will then be edited for you environment (you may prefer to prepare everything on your desktop then copy over to the deployment system)
```sh
wget https://raw.githubusercontent.com/dzl-dm/i2b2/master/docker-compose_use-with-secrets.yml ./docker-compose.yml
wget https://raw.githubusercontent.com/dzl-dm/i2b2/master/.env_use-with-secrets ./.env
```
> _NOTE:_ If you prefer to build the images yourself (or edit the way they are built), you shoud clone our whole repo: `git clone https://github.com/dzl-dm/i2b2.git`

We simply reference file based secrets for our sample, though docker allows more advanced solutions using password management systems. That is out of scope for this document.

In order to deploy properly, you must create some "secrets". Secrets are a term docker uses for private configuration values. The way they are stored and used provides separation from regular environment variables.

We also recommend making a few small adjustments to the settings.

### Create your secrets
The `secrets/` directory holds plain text files with sensitive information, usually passwords, which i2b2 uses. In our setup, these are loaded as env variables at container runtime in a more protected way than simply providing them docker as env vars. These are either referenced via the `.env` file by variables ending "_FILE" or as an additional file called `ENV_MULTILOAD.txt` which is sourced by the container at runtime. For obvious reasons, this is not provided in the repository, you must create this yourself. eg:
```sh
mkdir secrets; cd secrets; touch DB_ADMIN_PASS.txt DB_USER_PASS.txt POSTGRES_PASSWORD.txt ENV_MULTILOAD.txt; cd -
```
Your structure should look like:
```sh
tree ./i2b2/secrets/
├── DB_ADMIN_PASS.txt
├── POSTGRES_PASSWORD.txt
├── DB_USER_PASS.txt
└── ENV_MULTILOAD.txt
```
Then add information as described above, where the files referenced by `.env` should be individual passwords, eg `DB_ADMIN_PASS.txt`:
```txt
MySecurePassword
```

and `ENV_MULTILOAD.txt` file could look like:
```sh
export DS_PM_PASS=pm-pass
export DS_WD_PASS=wd-pass
...
```

## Adjust your .env
The directory has a `.env` file which is used by docker to put environment variables into the containers. It also allows the `docker compose` process to use them which can aid deployments. The .env provided covers a range of variables i2b2 can use, most of which will not need changing. For setting up on a remote server, the relevant fields should be changed.

## _Optional: Disable components_
If you do not intend to use a feature beyond the core components, you may disable the them by simply deleting the section from `docker-compose.yml`. See the list below for the names of the relevant sections, when deleting, include all sub-indented lines. If these components are not available, the bootstrap for the core components will gracefully ignore them.
* API (docker-compose section "i2b2-api")
  * metadata translator (docker-compose section "i2b2-meta")

## Deploy containers
Once all the settings are done, you can deploy the containers to your system. This follows regular docker commands so no special setup should be needed. From the base directory of the project (where you have the `docker-compose.yml` file), run the following:
```sh
docker compose up -d
```
> _NOTE:_ Older versions of docker do not include `compose` as a sub-command, you may need to install docker-compose separately and use the hyphenated command `docker-compose` in place of `docker compose`

After a short while, four(or three, without the api) containers should be running and a basic i2b2 installation available at the URL in the settings, by **default**: http://localhost/webclient. Demo data should be loaded if you set that option in `.env`

# Post install
Some changes cannot be reliably (or are not yet) integrated into the dockerization process, so should be done manually

## Change default passwords
There are 2 default users: "i2b2" and "AGG_SERVICE_ACCOUNT"

These both come with the default password "demouser". It important to change _both_ of these to reduce the risk of unwanted access. Unfortunately, the password hashing mechanism used by i2b2 is not directly re-producable in the bootstrap environment, meaning we can't (yet) set these automatically during first install, so they must be changed manually.

Login as the "i2b2" user choosing project "administration" (if an admin user has access to a project, they should have a choice directly after login). In the menu tree, navigate to "Manage Users -> i2b2 Admin" and use the form to change the password. Login again and do the same for user "AGG_SERVICE_ACCOUNT".

An additional step is required for "AGG_SERVICE_ACCOUNT". For this, some command line and SQL familiarity is beneficial.
* Connect to your host system (where you ran `docker compose ...`)
* Connect to the database container: `docker exec -it i2b2-database bash`
* Connect to the postgres DBMS: `sudo -u postgres psql i2b2`
* Run an update command _(replacing `${newpassword}` with what you set in the web interface)_: `update i2b2hive.hive_cell_params set value='${newpassword}' where param_name_cd='edu.harvard.i2b2.crc.pm.serviceaccount.password';`

# Where is demo data
Demo data is included in the database image - we get it as an archive file from i2b2's github pages: https://github.com/i2b2/i2b2-data ([More specifically](https://codeload.github.com/i2b2/i2b2-data/tar.gz/refs/tags/v1.7.12a.0001))

Only the "NewInstall" section is included and located (within the database container) at: `/docker-entrypoint/i2b2-data/edu.harvard.i2b2.data/Release_1-7/NewInstall/`

Using the `INCLUDE_DEMO_DATA=True` setting in the `.env` file, the demo data parts of this will automatically be installed, but the data can be accessed and run manually at any time after the containers and database structure have already been created.

> _NOTE:_ This process is not fully tested and may require some additional work

# Custom breakdowns
This is an i2b2 concern, so we won't repeat their information here, but rather link to the [relevant documentation](https://community.i2b2.org/wiki/display/RM/1.7.10+Release+Notes#id-1.7.10ReleaseNotes-SQLQueryBreakdowns) from i2b2.

## Direct SQL access
An alternative way to add custom breakdowns, or other custom data, is to access the database directly and run your own SQL files. This might suit your workflow better. In this case, you would need to expose the database port in the `docker-compose.yml` file, for postgresql (which we use), the lines should be under the ` i2b2-database:` section, eg:
```sh
    ports:
    - 5432:5432
```
The re-run docker compose:
```sh
docker compose up -d
```
and you can connect as if its a database running on the local system. See the secrets you created earlier for passwords for `postgres` (POSTGRES_PASSWORD.txt) and `i2b2` (DB_ADMIN_PASS.txt) users

# How to copy files into docker container
It is likely you will want to copy a file into the container at some point, for example SQL files to configure custom breakdowns. While this is not strictly good docker practice, it can be the most pragmatic option. This is achieved with a command similar to this from the host server (where you originally ran `docker compose ...`):
```sh
docker cp my-breakdown.sql i2b2-database:/tmp/
```

# Docker volumes
Docker uses "volumes" when persistant data should be stored, eg in the case of databases. This is essentially a directory on the host filesystem which is mounted as part of the container filesystem, meaning container re-creation does not affect the data.

## Where they're stored
By default, docker will place volumes under `/var/lib/docker/volumes/` on a posix system, the access to this under windows (even with WSL2) is different and outside the scope of this document. In case your docker system uses a different location you can check the docker root, which will contain a "volumes/" directory, with:
```sh
docker info | grep "Docker Root Dir:"
```

## Backing up and restoring volumes
These can be copied, archived, encrypted and sent to remote storage servers should you wish to maintain backups. Here is a possible way to manage that process (while you could chose other options!)

_"docker-vol-backup.sh"_
```sh
#!/usr/bin/env bash
## Backup all docker volumes - compressed and encrypted

>&2 echo -e "Starting docker backups... [$( date -Iminutes )]"
## Get location from docker info
src_vol_path="$( docker info 2>/dev/null | grep "Docker Root Dir" | cut -d\: -f2 )/volumes/"
docker_bak_pass="/home/backups/docker-config/secrets/backup_secret"
vol_backup_path="/home/backups/docker-backups/"
max_age=5 ## in days

cd $src_vol_path
vol_list=$( ls -1 ./ )
while read VOL; do
  if [ -d $VOL ] ; then
    >&2 echo "Backing up $VOL..."
    tar -cpz "$VOL" | openssl enc -aes-256-cbc -e -kfile "$docker_bak_pass" 2>/dev/null > ${vol_backup_path}$( basename "$VOL")_$( date -Iminutes )_${mode:-MANUAL}.tar.gz.enc
  fi
done <<< "$vol_list"

## Cleanup older backups
>&2 echo "Cleaning backups older than $max_age days"
cd $vol_backup_path
find $vol_backup_path -mindepth 1 -mtime +$max_age -delete
>&2 echo "Nightly docker backups complete [$( date -Iminutes )]"
```
> NOTE:_ Assumptions are made in this script about a "backups" user existing and the directory structure. This is run on the docker host and is not a related part of the docker system. We provide this example as a basic starting point

Also add a cron file, eg `/etc/cron.d/dockerbackup`
```sh
20 0 * * * root mode=CRON /home/backups/docker-config/docker-vol-backup.sh >>/var/log/docker-backups 2>&1
# Trigger the docker volume backup every night
```
### Restore
While the backup archive filename will contain metadata about the time of the backup, for readability, we copy the original backup archive to a simpler filename before restoring:
```sh
mv <dir_name>_<BAK_INFO>.tar.gz.enc <dir_name>_restore.tar.gz.enc
openssl enc -aes-256-cbc -d -in <dir_name>_restore.tar.gz.enc | tar xvzf <target_dir_name>
```
This will restore the archive data to the volume location (target_dir_name) - this is uaually best done while the container is stopped. Then the container can be restarted.

# REST API
We are introducing a REST API to provide programmable access to certain features. This complements the i2b2 product but is not required. It is currently a work in progress. Below are the feature which the API supports

## Metadata translator
This feature allows metadata from external sources to be translated and imported to the i2b2 tree so they can be used in the query builder.

The following external sources are supported

* ### Data which has been generated for [CoMetaR](https://github.com/dzl-dm/cometar) can be automatically fetched when it is updated (CoMetaR notifies the API)
For i2b2, the name and fuseki endpoint must be configured in `i2b2meta_user_config.yaml` under "fuseki_sources"
Additionally, the name must be configured for the CoMetaR system so it is recognised when notifying the API

* ### File based custom metadata
These are mounted under a volume defined in the `docker-compose.yml` file. The CSV files should reflect the structure of the i2b2 database, so its is a pre-requisite to have some familiarity with the database tables and structure. The sourcesystem_cd or "source ID" is taken from the directory name and automatically added to the data when it is added to i2b2

### Further information

In some cases, it may be necessary to manually trigger an update or remove data from i2b2. You can make an http get request to these API endpoints:
```sh
<i2b2-host>/i2b2-api/updatemeta/<source name> ## To fetch a source (defined in config or local CSV files)
<i2b2-host>/i2b2-api/flushmeta/<source name> ## To clear a source from the i2b2 database (it won't touch the source, regardless if that's a remote fuseki server or local csv files)
<i2b2-host>/i2b2-api/update-patient-counts ## i2b2 shows a matching patient count in parenthesis after each tree entry, this needs updating when the metadata changes (usually automatic)
```

