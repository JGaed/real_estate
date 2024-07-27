#!/bin/bash
CHROMEDRIVER_PATH='/home/omloo/projects/real_estate/chromedriver'

# Install Chrome.
sudo apt-get -y update
sudo apt-get -y install chromium-browser

# Install ChromeDriver.
wget -N https://github.com/electron/electron/releases/download/v31.3.0/chromedriver-v31.3.0-linux-arm64.zip -P /tmp
pwd=($pwd)
cd /tmp
unzip chromedriver*-arm64.zip -d ./unzip_chromedriver
rm chromedriver*-arm64.zip
cd ./unzip_chromedriver
sudo mv -f chromedriver $CHROMEDRIVER_PATH
USER=$(whoami)
sudo chown $USER:$USER $CHROMEDRIVER_PATH
sudo chmod 0755 $CHROMEDRIVER_PATH
cd $pwd
rm -rf /tmp/unzip_chromedriver*