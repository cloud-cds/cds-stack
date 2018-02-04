path=$(echo $1 | sed 's/\//\\\//g')
workspace=$2
ext=$3
sed -e "s/{folder}/$path/g" load_clarity_template_update.sql | sed -e "s/{workspace}/$workspace/g" | sed -e "s/{ext}/$ext/g" > load_clarity.$workspace.sql