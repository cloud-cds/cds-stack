#!/bin/bash
mkdir -p $2
for filename in *; do
  echo $filename
  awk 'NR==1{sub(/^\xef\xbb\xbf/,"")}{print}' $1/$filename > $2/$filename
done