FROM python:3.6-alpine
# Copy the main code base folder inside the container
COPY ./ml /ml


RUN apk add --update --no-cache \
        g++ pkgconfig ca-certificates \
        bash \
        postgresql-dev make \
        R R-dev R-doc \
    && ln -s /usr/include/locale.h /usr/include/xlocale.h \
    && pip install --upgrade pip \
    && pip install --no-cache-dir setuptools \
    && pip install -r /ml/requirements.txt \
    && pip install /ml

# Install R packages
RUN R CMD BATCH /ml/install-packages.R


# Set the default directory where CMD will execute
WORKDIR /ml

# Set the default command to execute
# when creating a new container
CMD ["sh", "run-ml.sh"]
