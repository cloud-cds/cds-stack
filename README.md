# trews_rest_api
The REST API to serve TREWS front-end 

### Set up
- Create a virtualenv called venv
- Pip Install `pip install -r requirements.txt`
- Set up some environment variables, contact Andong about credentials and which variables
- Run server! `gunicorn -b localhost:8000 trews:app`
