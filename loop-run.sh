#!/bin/bash

for i in {1..6}; do
    spark-submit  main1.py $i 
done