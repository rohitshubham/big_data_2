#!/bin/bash

#Installation on ubuntu

sudo apt-get update

curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -

sudo add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable"

sudo apt-get install docker-ce docker-ce-cli containerd.io

# run eclipse-mosquitto
sudo docker run -it -p 1883:1883 -p 9001:9001 -v /mosquitto/log eclipse-mosquitto