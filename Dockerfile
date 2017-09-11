FROM python:3.6.1-slim

# Copy the main code base folder inside the container
COPY dashan-universe/etl /etl
COPY dashan-universe/api /api
COPY dashan-universe/db /db
COPY dashan-universe/bin /bin
ADD dashan-universe/requirements.txt /

# Get pip to download and install requirements:
RUN apt-get update \
    && apt-get install -y build-essential libpq-dev rsyslog fuse postgresql-client graphviz \
        autoconf automake autotools-dev libtool pkg-config sudo strace git \
        # Install pyflame
    && git clone https://github.com/uber/pyflame.git && cd pyflame && ./autogen.sh && ./configure && make && make install && cd \
    && pip install --upgrade pip \
    && pip install --no-cache-dir setuptools \
    && pip install -r /requirements.txt \
    && pip install /etl
