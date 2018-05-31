sudo add-apt-repository -y ppa:jonathonf/python-3.6
sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
sudo apt-get update
sudo apt-get install -y postgresql-client \
  python3.6 python3.6-dev python3-pip git gcc-4.9 g++-4.9 \
  build-essential libpq-dev rsyslog fuse postgresql-client graphviz \
  autoconf automake autotools-dev libtool pkg-config strace docker.io \
  unzip

# install pyframe
git clone https://github.com/uber/pyflame.git && cd pyflame && ./autogen.sh && ./configure \
  && make && sudo make install && cd
sudo pip3 install virtualenvwrapper

VIRTUALENVWRAPPER_PYTHON='/usr/bin/python3' # This needs to be placed before the virtualenvwrapper command
source /usr/local/bin/virtualenvwrapper.sh

echo "export VIRTUALENVWRAPPER_PYTHON=\"/usr/bin/python3\"" >> ~/.bashrc
echo "source /usr/local/bin/virtualenvwrapper.sh" >> ~/.bashrc
echo "export EDITOR=vim" >> ~/.bashrc

mkvirtualenv -p python3.6 cds

sudo mv /usr/bin/x86_64-linux-gnu-gcc /usr/bin/x86_64-linux-gnu-gcc-bak
sudo ln -s /usr/bin/x86_64-linux-gnu-gcc-4.9 /usr/bin/x86_64-linux-gnu-gcc
pip3 install -r cds-stack/requirements.txt