#!/bin/bash

if [ "x$db_host" != "x" ]; then
  echo "Using DB config: $dw_host $db_user [hiding password]"
  sed "s/@@RDBHOST@@/$dw_host/; s/@@RBDUSER@@/$db_user/; s/@@RDBPW@@/$db_password/" < prod-dw.sql > deploy-prod-dw.sql
else
  echo "Invalid DB config: $dw_host $db_user"
  exit 1
fi
