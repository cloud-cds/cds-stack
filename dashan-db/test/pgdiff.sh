#!/bin/bash
#
# pgdiff.sh runs a compare on each schema type in the order that usually creates the fewest conflicts.
# At each step you are allowed to review and change the generated SQL before optionally running it.
# You are also allowed to rerun the diff on a type before continuing to the next type.  This is
# helpful when, for example dependent views are defined in the file before the view it depends on.
#
# If you convert this to a windows batch file (or, even better, a Go program), please share it.
#
# Example:
# pgdiff -U postgres -W supersecret -H dbhost1 -P 5432 -D maindb    -O 'sslmode=disable' \
#        -u postgres -w supersecret -h dbhost2 -p 5432 -d stagingdb -o 'sslmode=disable' \
#        COLUMN
#
HOST1=$1
NAME1=$2
USER1=$db_user
PORT1=$db_port
OPT1='sslmode=disable'

HOST2=$3
NAME2=$4
USER2=$db_user
PORT2=$db_port
OPT2='sslmode=disable'

echo "This is the reference database:"
echo "   ${USER1}@${HOST1}:${PORT1}/$NAME1"
# read -sp "Enter DB password: " passw
PASS1=$db_password
PASS2=$db_password

echo
echo "This database may be changed (if you choose):"
echo "   ${USER2}@${HOST2}:${PORT2}/$NAME2"
# read -sp "Enter DB password (defaults to previous password): " passw
# [[ -n $passw ]] && PASS2=$passw
# echo

let i=0
function rundiff() {
    ((i++))
    local TYPE=$1
    local sqlFile="${i}-${TYPE}.sql"
    local rerun=yes
    while [[ $rerun == yes ]]; do
        rerun=no
        echo "Generating diff for $TYPE... "
        pgdiff -U "$USER1" -W "$PASS1" -H "$HOST1" -P "$PORT1" -D "$NAME1" -O "$OPT1" \
                 -u "$USER2" -w "$PASS2" -h "$HOST2" -p "$PORT2" -d "$NAME2" -o "$OPT2" \
                 $TYPE > "$sqlFile"
        rc=$? && [[ $rc != 0 ]] && exit $rc
        if [[ $(cat "$sqlFile" | wc -l) -gt 4 ]]; then
            vi "$sqlFile"
            read -p "Do you wish to run this against ${NAME2}? [yN]: " yn
            if [[ $yn =~ ^y ]]; then
                PGPASSWORD="$PASS2" pgrun -U $USER2 -h $HOST2 -p $PORT2 -d $NAME2 -O "$OPT2" -f "$sqlFile"
                read -p "Rerun diff for $TYPE? [yN]: " yn
                [[ $yn =~ ^[yY] ]] && rerun=yes
            fi
        else
            read -p "No changes found for $TYPE (Press Enter) " x
        fi
    done
    echo
}

rundiff FUNCTION
rundiff ROLE
rundiff SEQUENCE
rundiff TABLE
rundiff COLUMN
rundiff VIEW
#rundiff OWNER
rundiff INDEX
rundiff FOREIGN_KEY
rundiff GRANT_RELATIONSHIP
rundiff GRANT_ATTRIBUTE
rundiff TRIGGER

echo "Done!"

