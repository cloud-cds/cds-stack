path=$(echo $1 | sed 's/\//\\\//g')
workspace=$2
ext=$3
#sed -e "s/{folder}/$path/g" load_clarity_template.sql | sed -e "s/{workspace}/$workspace/g" | sed -e "s/{ext}/$ext/g" > load_clarity.$workspace.sql
sed -e "s/{folder}/$path/g" load_clarity_template_jhh.sql | sed -e "s/{workspace}/$workspace/g" | sed -e "s/{ext}/$ext/g" > load_clarity.$workspace.sql