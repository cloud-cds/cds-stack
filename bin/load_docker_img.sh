sudo docker -t build trews-etl .. -f Dockerfile_c2dw
sudo docker save -o ~/zad/trews-etl.img trews-etl:latest
sudo scp -i ~/.ssh/tf-opsdx-dev ~/zad/trews-etl.img admin@$1:.
ssh -i ~/.ssh/tf-opsdx-dev admin@$1 'sudo docker load -i trews-etl.img'