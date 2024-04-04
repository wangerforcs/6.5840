#!/bin/bash
for((i=0;i<20;i++))
do 
    go test -run 3B
    sleep 0.5
done