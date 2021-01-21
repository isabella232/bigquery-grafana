# Source: https://github.com/grafana/grafana/blob/master/packaging/docker/Dockerfile

# Download Grafana
ARG BASE_IMAGE=gcr.io/shopify-base-images/alpine:latest
FROM ${BASE_IMAGE}

RUN apk add --update --no-cache tar

ENV GRAFANA_VERSION=7.3.6
ENV GRAFANA_TGZ=grafana-${GRAFANA_VERSION}.linux-amd64.tar.gz
ENV GRAFANA_TGZ_SOURCE=https://dl.grafana.com/oss/release/${GRAFANA_TGZ}

RUN wget ${GRAFANA_TGZ_SOURCE} && \
    mv ${GRAFANA_TGZ} /tmp/grafana.tar.gz && \
    mkdir /tmp/grafana && tar xzf /tmp/grafana.tar.gz --strip-components=1 -C /tmp/grafana


# Final image
FROM ${BASE_IMAGE}

ARG GF_UID="472"
ARG GF_GID="0"

ENV PATH=/usr/share/grafana/bin:${PATH} \
    GF_PATHS_CONFIG="/etc/grafana/grafana.ini" \
    GF_PATHS_DATA="/var/lib/grafana" \
    GF_PATHS_HOME="/usr/share/grafana" \
    GF_PATHS_LOGS="/var/log/grafana" \
    GF_PATHS_PLUGINS="/var/lib/grafana/plugins" \
    GF_PATHS_PROVISIONING="/etc/grafana/provisioning"

WORKDIR $GF_PATHS_HOME

RUN apk add --no-cache ca-certificates bash tzdata && \
    apk add --no-cache openssl musl-utils


RUN if [ `arch` = "x86_64" ]; then \
      apk add --no-cache libaio libnsl && \
      ln -s /usr/lib/libnsl.so.2 /usr/lib/libnsl.so.1 && \
      wget https://github.com/sgerrand/alpine-pkg-glibc/releases/download/2.30-r0/glibc-2.30-r0.apk \
        -O /tmp/glibc-2.30-r0.apk && \
      wget https://github.com/sgerrand/alpine-pkg-glibc/releases/download/2.30-r0/glibc-bin-2.30-r0.apk \
        -O /tmp/glibc-bin-2.30-r0.apk && \
      apk add --allow-untrusted /tmp/glibc-2.30-r0.apk /tmp/glibc-bin-2.30-r0.apk && \
      rm -f /tmp/glibc-2.30-r0.apk && \
      rm -f /tmp/glibc-bin-2.30-r0.apk && \
      rm -f /lib/ld-linux-x86-64.so.2 && \
      rm -f /etc/ld.so.cache; \
    fi

COPY --from=0 /tmp/grafana "$GF_PATHS_HOME"


RUN if [ ! $(getent group "$GF_GID") ]; then \
      addgroup -S -g $GF_GID grafana; \
    fi

RUN export GF_GID_NAME=$(getent group $GF_GID | cut -d':' -f1) && \
    mkdir -p "$GF_PATHS_HOME/.aws" && \
    adduser -S -u $GF_UID -G "$GF_GID_NAME" grafana && \
    mkdir -p "$GF_PATHS_PROVISIONING/datasources" \
             "$GF_PATHS_PROVISIONING/dashboards" \
             "$GF_PATHS_PROVISIONING/notifiers" \
             "$GF_PATHS_PROVISIONING/plugins" \
             "$GF_PATHS_LOGS" \
             "$GF_PATHS_PLUGINS" \
             "$GF_PATHS_DATA" && \
    cp "$GF_PATHS_HOME/conf/sample.ini" "$GF_PATHS_CONFIG" && \
    cp "$GF_PATHS_HOME/conf/ldap.toml" /etc/grafana/ldap.toml && \
    chown -R "grafana:$GF_GID_NAME" "$GF_PATHS_DATA" "$GF_PATHS_HOME/.aws" "$GF_PATHS_LOGS" "$GF_PATHS_PLUGINS" "$GF_PATHS_PROVISIONING" && \
    chmod -R 777 "$GF_PATHS_DATA" "$GF_PATHS_LOGS" "$GF_PATHS_PLUGINS" "$GF_PATHS_PROVISIONING"

EXPOSE 3000

COPY ./dist /var/lib/grafana/plugins/bq-plugin

COPY ./docker/run.sh /run.sh


USER "$GF_UID"
ENTRYPOINT [ "/run.sh" ]
