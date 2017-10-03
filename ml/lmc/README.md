Dashan ML
==========
Installation
---------------
### Install R
```bash
wget https://cran.rstudio.com/src/base/R-3/R-3.2.2.tar.gz
tar xvf R-3.1.1.tar.gz
cd R-3.2.2
./configure
make && make install
```
or
```bash
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E084DAB9
sudo add-apt-repository ppa:marutter/rdev

sudo apt-get update
sudo apt-get upgrade
sudo apt-get install r-base r-base-dev
```

### Install R packages
```bash
R CMD BATCH install-packages.R
```