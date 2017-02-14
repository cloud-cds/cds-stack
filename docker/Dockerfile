FROM ubuntu

RUN apt-get update

# Install basic applications
RUN apt-get install -y tar git curl nano wget dialog net-tools build-essential

# Install Python and Basic Python Tools
RUN apt-get install -y python python-dev python-distribute python-pip

RUN apt-get install -y python-psycopg2 libpq-dev 
# Copy the main code base folder inside the container
ADD /trews_rest_api /trews_rest_api

# Get pip to download and install requirements:
RUN pip install --upgrade pip
RUN pip install -r /trews_rest_api/requirements.txt




# Set the default directory where CMD will execute
WORKDIR /trews_rest_api

# Set the default command to execute
# when creating a new container
CMD gunicorn -b localhost:8000 trews:app


