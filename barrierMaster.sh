#!/bin/bash

echo hello
host1=$1
host2=$2
nc -l 2387 &
nc -l 2388 &
wait
echo done
echo ok | nc $host1 2397
echo ok | nc $host2 2398