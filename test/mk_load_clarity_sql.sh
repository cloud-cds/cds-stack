path=$(echo $1 | sed 's/\//\\\//g')
workspace=$2
ext=$3
sed -e "s/{folder}/$path/" load_clarity_template.sql | sed -e "s/{workspace}/$workspace/" | sed -e "s/{ext}/$ext/" > load_clarity.$workspace.sql