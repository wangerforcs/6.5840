#!/bin/bash
for((i=0;i<10;i++))
do 
    go test -run 3A
    sleep 0.5
done