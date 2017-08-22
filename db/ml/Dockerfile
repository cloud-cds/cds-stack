FROM ubuntu:14.04

RUN apt-get update \
    && apt-get install -y software-properties-common curl \
    && add-apt-repository ppa:jonathonf/python-3.6 \
    && apt-get update \
    && apt-get install -y python3.6 python3.6-dev python-dev \
    && curl -o /tmp/get-pip.py "https://bootstrap.pypa.io/get-pip.py" \
    && python3.6 /tmp/get-pip.py \
    && rm -rf /var/lib/apt/lists/* \
    && alias python=python3.6 \
    && echo "alias python=python3.6" >> /root/.bashrc


RUN apt-get update \
    && apt-get install -y software-properties-common curl \
    && add-apt-repository ppa:marutter/rrutter \
    && apt-get update \
    && apt-get install -y r-base r-base-dev

# Copy the main code base folder inside the container
COPY ./ml /ml

# Install python related libs
RUN apt-get install -y postgresql

RUN add-apt-repository ppa:ubuntu-toolchain-r/test \
    && apt-get update \
    && apt-get install -y gcc-4.9 \
    && rm /usr/bin/x86_64-linux-gnu-gcc \
    && ln -s /usr/bin/gcc-4.9 /usr/bin/x86_64-linux-gnu-gcc

RUN pip install --upgrade pip \
    && pip install --no-cache-dir setuptools \
    && pip install -r /ml/requirements.txt \
    && pip install /ml

# Set the default directory where CMD will execute
WORKDIR /ml
