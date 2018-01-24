FROM python:3.6.1-slim

# Copy the main code base folder inside the container
COPY dashan-universe/etl /etl
COPY dashan-universe/api /api
COPY dashan-universe/db /db
COPY dashan-universe/bin /bin
COPY dashan-universe/cloud/aws/stage3/jobs /jobs
ADD dashan-universe/requirements.txt /

# Get pip to download and install requirements:
RUN apt-get update \
    && apt-get install -y build-essential libpq-dev rsyslog fuse postgresql-client graphviz npm curl\
        autoconf automake autotools-dev libtool pkg-config sudo strace git vim\
        # Install pyflame
    && git clone https://github.com/uber/pyflame.git && cd pyflame && ./autogen.sh && ./configure && make && make install && cd \
    && pip install --upgrade pip \
    && pip install --no-cache-dir setuptools \
    && pip install -r /requirements.txt \
    && pip install /etl \
    && pip install aiobotocore \
    && curl -o- https://raw.githubusercontent.com/creationix/nvm/v0.33.6/install.sh | bash \
    && export NVM_DIR="$HOME/.nvm" && [ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh" \
    && nvm install node && cd /etl/events-soap && npm install && cd /
