sudo docker build trews-ml .. -f Dockerfile
sudo docker save -o ~/zad/trews-ml.img trews-ml:latest
sudo scp -i ~/.ssh/tf-opsdx-dev ~/zad/trews-ml.img admin@$1:.
ssh -i ~/.ssh/tf-opsdx-dev admin@$1 'sudo docker load -i trews-ml.img'