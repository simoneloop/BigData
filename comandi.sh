#!/bin/bash

sudo apt-get update
sudo apt-get upgrade

sudo apt install python3-pip

pip install selenium
pip install pandas
pip install webdriver-manager


wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
#sudo dpkg -i google-chrome-stable_current_amd64.deb
sudo apt install ./google-chrome-stable_current_amd64.deb

