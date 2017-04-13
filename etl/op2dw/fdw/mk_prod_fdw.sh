#!/bin/bash

if [ "x$db_host" != "x" ]; then
  echo "Using DB config: $db_host $db_user [hiding password]"
  sed "s/@@RDBHOST@@/$db_host/; s/@@RDBUSER@@/$db_user/; s/@@RDBPW@@/$db_password/" < prod.sql > deploy-prod.sql
else
  echo "Invalid DB config: $db_host $db_user"
  exit 1
fi
