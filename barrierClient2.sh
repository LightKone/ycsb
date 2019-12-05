#!/bin/bash

echo hello
master=$1
nc -l 2398 &
f=$!
a=$(ps | grep -o "^ *$f " | wc -l)
while [ $a -eq 1 ]
do
  echo ok | nc $master 2388
  sleep 1
  a=$(ps | grep -o "^ *$f " | wc -l)
done

echo go2