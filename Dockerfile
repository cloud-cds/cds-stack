# Base image built from images/dockerfiles/base
FROM yanif/dashan-universe-base:latest

# Copy the main code base folder inside the container
COPY cds-stack/etl /etl
COPY cds-stack/api /api
COPY cds-stack/db /db
COPY cds-stack/bin /bin
COPY cds-stack/cloud/aws/stage3/jobs /jobs
ADD cds-stack/requirements.txt /

# Get pip to download and install requirements:
RUN pip install /etl \
    && export NVM_DIR="$HOME/.nvm" && [ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh" \
    && cd /etl/events-soap && npm install && cd /
