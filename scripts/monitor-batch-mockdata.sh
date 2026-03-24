#!/bin/bash


 BUS_DT=$1

 if [[ -z $BUS_DT ]]; then
         echo "--ERROR-- Requires a date"
         exit 1
 fi

INPUT_DIR=/home/ubuntu/dedup-batch/data/input/${BUS_DT}

function getTableCount() {
        find $INPUT_DIR -name "*" -type d -print0  | while IFS= read -r -d '' filename; do
                if [[ "${filename}" != "${INPUT_DIR}" ]]; then
                        table=$(basename $filename)

                        rec_count=$(wc -l ${INPUT_DIR}/${table}/*.json | tail -1 | awk -F" " '{ print $1 }')

                        echo "$(date) - ${table} : ${rec_count}"
                fi
        done

        echo ""
}



 while true;
 do
         clear

         echo "$(date) - Getting # of records generated so far by table ..."
         echo "$(getTableCount)"


        total_num_tables=$(ls -l $INPUT_DIR/ | wc -l | awk -F" " '{ print $1 }')
        total_num_tables=$((total_num_tables-1))


         total_record_count=$(wc -l ${INPUT_DIR}/*/*.json | tail -1 | awk -F" " '{ print $1 }')
         echo "$(date) - $total_record_count records have been generated for $total_num_tables so far"

         echo ""
         if [[ -f ${INPUT_DIR}/${BUS_DT}.ready ]]; then
                 echo "$(date) - ${INPUT_DIR}/${BUS_DT}.ready Found! You can load it now."
                 echo ""
                 break
         else
                 echo "$(date) - ${INPUT_DIR}/${BUS_DT}.ready NOT FOUND!"
         fi

         echo ""
         echo "$(date) - Going to sleep for 120 seconds"
         echo "$(date) - --------------------------------------------"

         sleep 120
 done


exit 0
