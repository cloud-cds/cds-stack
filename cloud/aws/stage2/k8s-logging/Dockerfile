FROM fluent/fluentd:latest
WORKDIR /home/fluent
ENV PATH /home/fluent/.gem/ruby/2.3.0/bin:$PATH
ENV LANGUAGE en_US:en
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE UTF-8

USER root

COPY ./setup.sh /home/fluent/setup.sh

RUN apk --no-cache --update add sudo build-base git ruby-dev ruby-io-console ruby-rake libffi-dev && \
    sudo -u fluent gem install bundler fluent-plugin-record-reformer fluent-plugin-kubernetes_metadata_filter && \
    sudo -u fluent  /home/fluent/setup.sh && \
    rm -rf /home/fluent/.gem/ruby/2.3.0/cache/*.gem /home/fluent/setup.sh && \
    sudo -u fluent gem sources -c && \
    apk del sudo build-base git ruby-dev ruby-io-console ruby-rake && rm -rf /var/cache/apk/*

RUN mkdir -p /mnt/pos
EXPOSE 24284

RUN mkdir -p /fluentd/conf.d && \
    mkdir -p /fluentd/etc && \
    mkdir -p /fluentd/plugins

# Default settings
ENV CW_LOG_GROUP "k8s-logs"
ENV LOG_FORMAT "json"
ENV MAX_EVENTS_PER_BATCH 10000
ENV SOURCE_CATEGORY "%{namespace}/%{pod_name}"
ENV SOURCE_CATEGORY_PREFIX "kubernetes/"
ENV SOURCE_CATEGORY_REPLACE_DASH "/"
ENV SOURCE_NAME "%{namespace}.%{pod}.%{container}"
ENV KUBERNETES_META "true"

COPY ./conf.d/* /fluentd/conf.d/
COPY ./etc/* /fluentd/etc/
COPY ./plugins/* /fluentd/plugins/

CMD exec fluentd -c /fluentd/etc/$FLUENTD_CONF -p /fluentd/plugins $FLUENTD_OPT