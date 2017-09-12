FROM ubuntu:14.04
RUN apt-get update \
    && apt-get install -y software-properties-common curl \
    && add-apt-repository ppa:jonathonf/python-3.6 \
    && add-apt-repository ppa:marutter/rrutter \
    && add-apt-repository ppa:ubuntu-toolchain-r/test \
    && apt-get update \
    && apt-get install -y build-essential libpq-dev rsyslog fuse python3.6 python3.6-dev python-dev r-base r-base-dev postgresql-client gcc-4.9 python3-tk \
    && rm /usr/bin/x86_64-linux-gnu-gcc \
    && ln -s /usr/bin/gcc-4.9 /usr/bin/x86_64-linux-gnu-gcc \
    && curl -o /tmp/get-pip.py "https://bootstrap.pypa.io/get-pip.py" \
    && python3.6 /tmp/get-pip.py \
    && rm -rf /var/lib/apt/lists/* \
    && alias python=python3.6 \
    && echo "alias python=python3.6" >> /root/.bashrc

COPY dashan-universe/bin /bin
COPY dashan-universe/ml/trews /trews

RUN pip install --upgrade pip \
    && pip install --no-cache-dir setuptools \
    && pip install -r /trews/requirements.txt

RUN R CMD BATCH /trews/install-packages.R
RUN chmod +x /trews/run_ml.sh
# Set the default directory where CMD will execute
WORKDIR /trews