#!/bin/bash
CHROMEDRIVER_PATH='/home/omloo/projects/real_estate/chromedriver'

# Install Chrome.
sudo dnf -y update
sudo dnf -y install chromium

# Install ChromeDriver.
wget -N https://github.com/electron/electron/releases/download/v32.1.2/chromedriver-v32.1.2-linux-arm64.zip -P /tmp
pwd=$(pwd)
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
