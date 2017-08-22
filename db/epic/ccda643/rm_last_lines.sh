#!/bin/bash
mkdir -p $2
cd $1
for filename in *; do
  head -n -1 $filename > ../$2/$filename
done
head -n -1 hist.rpt > ../$2/hist.rpt