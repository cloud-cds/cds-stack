FROM python:3.6.1-slim

# Copy the main code base folder inside the container
COPY ./dashan-etl /dashan-etl
COPY ./dashan-db /dashan-db

# Get pip to download and install requirements:
RUN apt-get update \
    && apt-get install -y g++ libpq-dev rsyslog fuse postgresql-client graphviz\
    && pip install --upgrade pip \
    && pip install --no-cache-dir setuptools \
    && pip install -r /dashan-etl/requirements.txt \
    && pip install /dashan-etl

# Set the default directory where CMD will execute
WORKDIR /dashan-etl

# Set the default command to execute
# when creating a new container
CMD ["python", "./etl/epic2op/engine.py"]
