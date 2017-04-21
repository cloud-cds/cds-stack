FROM python:3.6-alpine
# Copy the main code base folder inside the container
COPY ./ml /ml

# Install python related libs
RUN apk add --update --no-cache \
        g++ pkgconfig ca-certificates \
        bash \
        postgresql-dev make \
    && ln -s /usr/include/locale.h /usr/include/xlocale.h \
    && pip install --upgrade pip \
    && pip install --no-cache-dir setuptools \
    && pip install -r /ml/requirements.txt \
    && pip install /ml

# Install R
RUN apk add --no-cache R R-dev R-doc curl libressl-dev curl-dev libxml2-dev gcc g++ git coreutils bash ncurses

RUN R -q -e "install.packages('Rcpp', repos = 'https://cloud.r-project.org/')" &&\
  rm -rf /tmp/*

RUN curl -L -O https://cran.r-project.org/src/contrib/httpuv_1.3.3.tar.gz &&\
  tar xvf httpuv_1.3.3.tar.gz &&\
  sed -i -e 's/__USE_MISC/_GNU_SOURCE/g' httpuv/src/libuv/src/fs-poll.c &&\
  tar -cf httpuv_1.3.3.tar.gz httpuv &&\
  R CMD INSTALL httpuv_1.3.3.tar.gz &&\
  rm -rf httpuv_1.3.3.tar.gz httpuv /tmp/*

RUN git clone https://github.com/ropensci/git2r.git &&\
  R CMD INSTALL --configure-args="--with-libssl-include=/usr/lib/" git2r &&\
  rm -rf git2r /tmp/*

RUN R -q -e "install.packages(c('devtools', 'covr', 'roxygen2', 'testthat'), repos = 'https://cloud.r-project.org/')" &&\
  rm -rf httpuv /tmp/*

# Install R packages
RUN R CMD BATCH /ml/install-packages.R


# Set the default directory where CMD will execute
WORKDIR /ml

# Set the default command to execute
# when creating a new container
CMD ["sh", "run-ml.sh"]
