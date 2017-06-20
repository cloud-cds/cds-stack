#!/bin/bash
mkdir -p $2
cd $1
mv flowsheet_dict.rpt ../$2/
mv lab_dict.rpt ../$2/
mv lab_proc.rpt ../$2/
for filename in *; do
  echo $filename
  echo $(echo $filename | cut -f1,3 -d.)
  if [[ $filename == *"hist"* ]]; then
          head -n -2 $filename >> ../$2/$(echo $filename | cut -f1,3 -d.)
  else
          head -n -1 $filename >> ../$2/$(echo $filename | cut -f1,3 -d.)
  fi
done