#!/bin/bash

# lent a cause de lantece : trop de requetes vers le namenode
#find /usr/ "*.[cpp,hh,c,h]" -exec hdfs dfs -appendToFile {} hdfs://buffet:9000/users/bfaltrep/sourcefile.txt \;

#find /usr/ "*.[cpp,hh,c,h]" -exec cat {} >> sourcefile.txt 2> /dev/null \;

find . -type f -exec cat {} >> source 2> /dev/null \;
hdfs dfs -copyFromLocal source  hdfs://lentix:9000/source
rm source
