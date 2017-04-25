path=$(echo $1 | sed 's/\//\\\//g')
sed -e "s/{folder}/$path/" load_clarity_template.sql > load_clarity.$2.sql