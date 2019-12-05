#!/bin/bash

echo hello
master=$1
nc -l 2397 &
f=$!
a=$(ps | grep -o "^ *$f " | wc -l)
while [ $a -eq 1 ]
do
  echo ok | nc $master 2387
  sleep 1
  a=$(ps | grep -o "^ *$f " | wc -l)
done

echo go1c