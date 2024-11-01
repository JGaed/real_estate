#!/bin/bash
CHROMEDRIVER_PATH='/home/omloo/projects/real_estate/chromedriver'

# Install Chrome.
sudo apt -y update
sudo apt -y install chromium

get_latest_release() {
  curl --silent "https://api.github.com/repos/$1/releases/latest" | # Get latest release from GitHub api
    grep '"tag_name":' |                                            # Get tag line
    sed -E 's/.*"([^"]+)".*/\1/'                                    # Pluck JSON value
}

# Install ChromeDriver.
latest_version=$(get_latest_release electron/electron)
#latest_version="v32.2.0"
wget -N https://github.com/electron/electron/releases/download/$latest_version/chromedriver-$latest_version-linux-arm64.zip -P /tmp
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
