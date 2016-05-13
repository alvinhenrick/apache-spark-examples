#!/usr/bin/env bash

/opt/spark-1.6.1/bin/spark-submit --master spark://spark-master.hadoop:7077 --class com.wiki.produce.example.WikiProduceStream --deploy-mode client --driver-memory 4g --executor-memory 2g --executor-cores 2 target/dfw-spark-meetup.jar