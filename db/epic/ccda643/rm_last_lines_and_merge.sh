#!/bin/bash
mkdir -p $2 # output location
cd $1 # directory of file

head -n -1 flowsheet_dict.rpt > ../$2/flowsheet_dict.rpt
head -n -1 lab_dict.rpt > ../$2/lab_dict.rpt
head -n -1 lab_proc.rpt > ../$2/lab_proc.rpt
for filename in *; do
  echo $filename
  echo $(echo $filename | cut -f1,3 -d.)
  if [[ $filename == *"hist"* ]]; then
          head -n -2 $filename >> ../$2/$(echo $filename | cut -f1,3 -d.)
  else
          head -n -1 $filename >> ../$2/$(echo $filename | cut -f1,3 -d.)
  fi
done