FROM python:3.6.1

ENV WORKDIR  /pyflame/
ENV prometheus_multiproc_dir /tmp

WORKDIR $WORKDIR
RUN apt-get update && \
    apt-get install autoconf automake autotools-dev g++ libtool pkg-config sudo strace -y && \
    rm -rf /var/lib/apt/lists/* && \
    # Install pyflame
    git clone https://github.com/uber/pyflame.git . && ./autogen.sh && ./configure && make && make install && rm -rf $WORKDIR

ADD . /trews_rest_api

RUN pip install -r /trews_rest_api/requirements.txt

WORKDIR /trews_rest_api
CMD [ "sh", "start-dev.sh" ]
