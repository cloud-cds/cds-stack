# Base image built from images/dockerfiles/base
FROM yanif/dashan-universe-base:latest

# Copy the main code base folder inside the container
COPY dashan-universe/etl /etl
COPY dashan-universe/api /api
COPY dashan-universe/db /db
COPY dashan-universe/bin /bin
COPY dashan-universe/cloud/aws/stage3/jobs /jobs
ADD dashan-universe/requirements.txt /

# Get pip to download and install requirements:
RUN pip install /etl \
    && export NVM_DIR="$HOME/.nvm" && [ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh" \
    && cd /etl/events-soap && npm install && cd /
