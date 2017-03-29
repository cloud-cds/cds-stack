FROM python:3.6

# Copy the main code base folder inside the container
COPY . /dashan-etl
COPY ../dashan-db /dashan-db

# Get pip to download and install requirements:
RUN pip install -r /dashan-etl/requirements.txt && \
    pip install /dashan-etl

# Set the default directory where CMD will execute
WORKDIR /dashan-etl

# Set the default command to execute
# when creating a new container
CMD ["python", "./etl/epic2op/engine.py"]
