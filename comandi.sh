#!/bin/bash

sudo apt-get update
sudo apt-get upgrade

git clone https://github.com/simoneloop/BigData.git

wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo dpkg -i google-chrome-stable_current_amd64.deb

sudo apt install python3-pip

