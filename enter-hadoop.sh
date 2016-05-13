#!/usr/bin/env bash
#docker run -it --rm --env-file=./hadoop.env --net hadoop awesomedata/hadoop hadoop fs -mkdir -p /user/root

ID=$1
docker run -it --rm --env-file=/home/alvin/IdeaProjects/awesome-data/hadoop.env --net hadoop --name temp-hadoop-${ID} awesomedata/hadoop bash