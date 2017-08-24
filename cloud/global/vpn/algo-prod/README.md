# VPN Installation
1. Download algo and its requirements: https://github.com/trailofbits/algo.
2. Change into the newly created `algo-master` directory.
3. Copy the cloud-ec2 directory to `algo-master` directory:
```
cp -r cloud-ec2/defaults/main.yml algo-master/roles/cloud-ec2/defaults/main.yml
cp -r cloud-ec2/tasks/main.yml algo-master/roles/cloud-ec2/tasks/main.yml
```
