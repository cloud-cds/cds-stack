FROM python:3.6-alpine
# Copy the main code base folder inside the container
COPY ./ml /ml

# Install R
ARG R_VERSION

ENV R_VERSION ${R_VERSION:-3.3.1}

RUN apk --no-cache add build-base gfortran readline-dev \
       icu-dev bzip2-dev xz-dev pcre-dev \
       libjpeg-turbo-dev libpng-dev tiff-dev  \
       curl-dev zip file coreutils bash \
    && apk --no-cache add --virtual build-deps \
       curl perl openjdk8-jre-base pango-dev cairo-dev \
    && cd /tmp \
    && curl -O https://cran.r-project.org/src/base/R-${R_VERSION:0:1}/R-${R_VERSION}.tar.gz \
    && tar -xf R-${R_VERSION}.tar.gz \
    && cd R-${R_VERSION} \
    && CFLAGS="-g -O2 -D_DEFAULT_SOURCE" \
       CXXFLAGS="-g -O2 -D__MUSL__" \
       ./configure --prefix=/usr \
                   --sysconfdir=/etc/R \
                   --localstatedir=/var \
                   rdocdir=/usr/share/doc/R \
                   rincludedir=/usr/include/R \
                   rsharedir=/usr/share/R \
                   --disable-nls \
                   --without-x \
                   --without-recommended-packages \
                   --enable-memory-profiling \
    && make \
    && make install \
    && apk del --purge --rdepends build-deps \
    && rm -f /usr/lib/R/bin/R \
    && ln -s /usr/bin/R /usr/lib/R/bin/R \
    && echo "R_LIBS_SITE=\${R_LIBS_SITE-'/usr/local/lib/R/site-library:/usr/lib/R/library'}" >> /usr/lib/R/etc/Renviron \
    && echo 'options(repos = c(CRAN = "https://cloud.r-project.org/"))' >> /usr/lib/R/etc/Rprofile.site \
    && mkdir -p /etc/R \
    && ln -s /usr/lib/R/etc/* /etc/R/ \
    && mkdir -p /usr/local/lib/R/site-library \
    && chgrp users /usr/local/lib/R/site-library \
    && rm -rf /usr/lib/R/library/translations \
    && rm -rf /tmp/*

# Install R packages
RUN R CMD BATCH /ml/install-packages.R

# Install python related libs
RUN apk add --update --no-cache \
        g++ pkgconfig ca-certificates \
        bash \
        postgresql-client make \
    && ln -s /usr/include/locale.h /usr/include/xlocale.h \
    && pip install --upgrade pip \
    && pip install --no-cache-dir setuptools \
    && pip install -r /ml/requirements.txt \
    && pip install /ml

# Set the default directory where CMD will execute
WORKDIR /ml

# Set the default command to execute
# when creating a new container
CMD ["sh", "run-ml.sh"]
