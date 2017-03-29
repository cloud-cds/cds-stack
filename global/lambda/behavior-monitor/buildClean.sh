chmod 777 service.py
rm -rf dist
lambda build
chmod -R 777 dist
