FROM python:3.6-alpine

# Copy the main code base folder inside the container
COPY ./dashan-etl /dashan-etl
COPY ./dashan-db /dashan-db

RUN apk add --update --no-cache \
        g++ pkgconfig ca-certificates \
        postgresql-dev make \
    && ln -s /usr/include/locale.h /usr/include/xlocale.h \
    && pip install --upgrade pip \
    && pip install --no-cache-dir setuptools \
    && pip install -r /dashan-etl/requirements.txt \
    && pip install /dashan-etl

# Set the default directory where CMD will execute
WORKDIR /dashan-etl

# Set the default command to execute
# when creating a new container
CMD ["python", "./etl/epic2op/engine.py"]
