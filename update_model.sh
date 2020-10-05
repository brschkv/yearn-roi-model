#!/bin/bash

git pull
docker build -t yearnroi-model:prod ./model/
docker run -it --env-file .env yearnroi-model:prod
