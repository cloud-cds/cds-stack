sudo docker build -t trews-ml .. -f Dockerfile
sudo docker save -o ~/trews-ml.img trews-ml:latest
sudo scp -i ~/.ssh/tf-opsdx-dev ~/trews-ml.img admin@$1:.
ssh -i ~/.ssh/tf-opsdx-dev admin@$1 'sudo docker load -i trews-ml.img'