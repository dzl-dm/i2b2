ARG WEB_TAG=${WEB_TAG:-'release-v1.7.12a.0002'}
FROM i2b2/i2b2-web:${WEB_TAG}

## System env
ENV TZ=${TZ:-'Europe/Berlin'}

WORKDIR /docker-entrypoint
## Run some generic fixes
RUN cd /var/www/html/ && \
    ## statusText is not available with http2, the code is available for all http protocols (https://github.com/i2b2/i2b2-webclient/commit/63ddf0cedcb6f92f71621b73dcf4c9b9bf45bc5b)
    grep -rl "if (response.transport.statusText==\"OK\") {" * | xargs sed -i 's/if (response.transport.statusText=="OK") {/if (response.transport.status == 200) {/g' && \
    ## Use dockerized, seperate wildfly service
    ls -1 {admin,webclient}/index.php | xargs sed -i 's*$pmURL = "http://127.0.0.1:8080/i2b2/rest/PMService/getServices";*$pmURL = "http://i2b2-wildfly:8080/i2b2/rest/PMService/getServices";*g' && \
    ## User our own wildfly instance, not i2b2.org
    find  . -maxdepth 2 -type f -name i2b2_config_data.js -exec sed -i 's*urlCellPM: "http://services.i2b2.org/i2b2/services/PMService/",*urlCellPM: "http://i2b2-wildfly:8080/i2b2/services/PMService/",*g' {} \; && \
    ## Disable certain elements from the admin interface - as per docs
    sed -i -e 's*{ code: "ONT"   },*//{ code: "ONT"   },*g' -e 's*{ code: "CRC"   },*//{ code: "CRC"   },*g' -e 's*{ code: "WORK"   },*//{ code: "WORK"   },*g' admin/js-i2b2/i2b2_loader.js

COPY ./bootstrap-web.sh ./bootstrap.sh
COPY ./conf ./conf
COPY ./favicon.ico /var/www/html/

## Override entrypoint for custom initialisation...
ENTRYPOINT ["/docker-entrypoint/bootstrap.sh"]
## From upstream...
CMD ["/run-httpd.sh"]
