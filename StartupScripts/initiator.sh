#!/bin/bash

echo Please Enter External IP:
read natIP


echo 1.Wordcount
echo 2.Inverted Index
echo Please enter your choice:
read choice

if [ $choice -eq 1 ]
then
    curl -v http://$natIP:7001/ -F datafile=@wordConfig.json -o word_count.txt

elif [ $choice -eq 2 ]
then
    curl -v http://$natIP:7001/ -F datafile=@invertedConfig.json -o inverted_index.txt

else
    echo Invalid choice. Please try again...
fi
