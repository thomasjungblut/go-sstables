#!/bin/bash

ex_files=$(ls _examples/*.go)
for f_name in $ex_files; do
  echo "running $f_name"
  go run $f_name
  if [ $? -ne 0 ]; then
    echo "failure while running $f_name"
    exit 1
  fi
done
