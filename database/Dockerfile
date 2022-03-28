FROM alpine:3.14 AS i2b2-data-fetch
## Multi-stage build to add remote archive is supposed to be more efficient than curl from the main image

WORKDIR /i2b2-data
# ADD https://codeload.github.com/i2b2/i2b2-data/tar.gz/refs/tags/v1.7.12a.0001 ./i2b2-data.tar.gz
RUN wget https://codeload.github.com/i2b2/i2b2-data/tar.gz/refs/tags/v1.7.12a.0001 ./i2b2-data.tar.gz
RUN tar xzf ./i2b2-data.tar.gz
RUN rm -rf ./i2b2-data.tar.gz 

FROM postgres:12

ENV TZ=${TZ:-'Europe/Berlin'}
ARG DEBIAN_FRONTEND=noninteractive
RUN apt update; apt install -y sudo unzip; apt-get clean; rm -rf /var/lib/apt/lists/*

WORKDIR /docker-entrypoint
## Need only SQL files for db init - i2b2 package uses ant and contains other artifacts - a huge archive
COPY --from=i2b2-data-fetch /i2b2-data/i2b2-data-1.7.12a.0001/edu.harvard.i2b2.data/Release_1-7/NewInstall ./i2b2-data/edu.harvard.i2b2.data/Release_1-7/NewInstall
COPY ./bootstrap-postgres.sh ./bootstrap.sh
COPY ./check-db.sh ./check-db.sh
COPY ./init-postgres.sh ./init-db.sh
COPY ./sql ./initdb.d
COPY ./healthcheck.sh ./

## Check i2b2 database exists with the last loaded schema, then check there are no default "demouser" passwords
HEALTHCHECK --interval=15s --timeout=10s --retries=45 \
    CMD ./healthcheck.sh
## Override entrypoint for custom initialisation...
ENTRYPOINT ["/docker-entrypoint/bootstrap.sh"]
CMD ["postgres"]
