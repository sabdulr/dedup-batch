#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR=$(dirname "$SCRIPT_DIR")

LOG_PATH=${SCRIPT_DIR}/logs
service_app="generate-mock-data-scripts"
LOGFILE=$LOG_PATH/$service_app.log

FROM_DATE=$(date "+%Y-%m-%d")
DAYS=15
RECORDS=6000000

FIRST_RUN=""

PROFILE=prod
APP_DEBUG=false
INPUT_DIR=${APP_DIR}/data/input
OUTPUT_DIR=${APP_DIR}/data/output
WORK_DIR=${APP_DIR}/data/db-temp/${PROFILE}
LOOKBACK_DAYS=3
TABLES_FILE=${APP_DIR}/config/tables.txt

GEN_SCR_DIR=${SCRIPT_DIR}/generated

MOCK_DATA_SCRIPT=${GEN_SCR_DIR}/1-batch-mock-data.sh
BATCH_RUN_SCRIPT=${GEN_SCR_DIR}/2-batch-load.sh

if [[ ! -d ${INPUT_DIR} ]]; then
	mkdir -p ${INPUT_DIR}
fi

if [[ ! -d ${OUTPUT_DIR} ]]; then
	mkdir -p ${OUTPUT_DIR}
fi

if [[ ! -d ${WORK_DIR} ]]; then
	mkdir -p ${WORK_DIR}
fi

if [[ ! -d ${GEN_SCR_DIR} ]]; then
	mkdir -p ${GEN_SCR_DIR}
fi


function log() {
        level=$1
        message=$2

        if [[ "${debug}" == "true" ]] ; then
                echo "$(date '+%Y-%m-%d %H:%M:%S') - $level - $message" | tee -a $LOGFILE
        else
                if [[ "${level}" != "DEBUG" ]] ; then
                        echo "$(date '+%Y-%m-%d %H:%M:%S') - $level - $message" | tee -a $LOGFILE
                fi
        fi

}

function create_mock_data_scr() {
	local run_date=$1

	log "INFO" "Creating 2-batch-mock-data.sh for $run_date"

	for table in "${tables_list[@]}"; do
		if [[ "${gen}" == "rand" ]]; then
			RECORDS=$((250000 + RANDOM % 400000))
		fi

		echo "Create Table Script for $table : $run_date : ${RECORDS}"

		echo "${SCRIPT_DIR}/generate-mock-data.sh ${table} ${run_date} ${RECORDS} ${INPUT_DIR}/${run_date}/${table}" >> "${MOCK_DATA_SCRIPT}"
	done

	echo "" >> "${MOCK_DATA_SCRIPT}"
}

log "INFO" "Starting Reporting daily script"

SHORT=f,d:,p:,x,b:,a:,r:,h
LONG=first-run,from-date:,profile:,debug,lookback-days:,days:,records:,help
OPTS=$(getopt -a -n $service_app --options $SHORT --longoptions $LONG -- "$@")

eval set -- "$OPTS"

while :
do
  case "$1" in
    -f | --first-run )
      log "INFO" "Data will be loaded fresh!"
      FIRST_RUN="true"
      shift 1
      continue
      ;;
    -d | --from-date )
      FROM_DATE="$2"
      shift 2
      ;;
    -p | --profile )
      PROFILE="$2"
      shift 2
      ;;
    -x | --debug )
      log "INFO" "DEBUG is on"
          APP_DEBUG=true
          shift 1
          continue
      ;;
    -b | --lookback-days )
      LOOKBACK_DAYS="$2"
      shift 2
      ;;
    -a | --days )
      DAYS="$2"
      shift 2
      ;;
    -r | --records )
      RECORDS="$2"
      shift 2
      ;;
    -h | --help)
      echo "This script runs with no arguments to produce report for previous date or with a single argument"
      echo "-d --reporting-date <YYYY-MM-DD>"
      echo "-h --help print this help"
      exit 2
      ;;
      --)
      shift;
      break
      ;;
    *)
      echo "Unexpected option: $1"
      exit 3
      ;;
  esac
done

gen=""
if [[ "$RECORDS" == "rand" ]]; then
	gen="rand"
	RECORDS=$((200000 + RANDOM % 400000))
fi

log "INFO" "-------------"
log "INFO" "FROM_DATE: $FROM_DATE"
log "INFO" "DAYS: $DAYS"
log "INFO" "RECORDS: $RECORDS"
log "INFO" "FIRST_RUN: $FIRST_RUN"
log "INFO" "PROFILE: $PROFILE"
log "INFO" "APP_DEBUG: $APP_DEBUG"
log "INFO" "INPUT_DIR: $INPUT_DIR"
log "INFO" "OUTPUT_DIR: $OUTPUT_DIR"
log "INFO" "WORK_DIR: $WORK_DIR"
log "INFO" "LOOKBACK_DAYS: $LOOKBACK_DAYS"
log "INFO" "TABLES_FILE: $TABLES_FILE"

log "INFO" "MOCK_DATA_SCRIPT: $MOCK_DATA_SCRIPT"
log "INFO" "BATCH_RUN_SCRIPT: $BATCH_RUN_SCRIPT"

# Declare and initialize an array
tables_list=("account" "accountaccesstype" "accountbalance" "accountdormancystatus" "accounthistory" "accountsegment" "activitylogcategory" "activitylogtype" "addresstype" "award" "awardtype" "sars_account" "sars_customer" "sars_customeraccount" "sars_plastichistory" "sars_product" "cardstatus" "communicationpreference" "communicationpreferencestatus" "contacttype" "country" "culture" "customer" "customeraccount" "customeraddress" "customerbalance" "customercontact" "customerprimaryaddress" "customersource" "enrollmentstatus" "gender" "mcclist" "mcctype" "order" "orderdetailtracking" "orderdetailtrackingstatus" "orderdetailtrackinghistory" "phonenumbertype" "point" "pointcategory" "pointtype" "province" "segment" "transactionreasoncode")




date_list=()
# 2. Loop through a sequence of items (e.g., numbers)
for ((delta=0; delta<DAYS; delta++)); do
  run_date=$(date -d "$FROM_DATE +$delta days" +"%Y-%m-%d")
  
  date_list+=("$run_date")
  run_date=""
done

##################
if [[ -f ${MOCK_DATA_SCRIPT} ]]; then
	rm ${MOCK_DATA_SCRIPT}
fi


echo "#!/bin/bash" > "${MOCK_DATA_SCRIPT}"
echo "" >> "${MOCK_DATA_SCRIPT}"

echo "Looping through date list:"
for dt in "${date_list[@]}"; do
        create_mock_data_scr "${dt}"
	echo "echo \"ready!\" > ${INPUT_DIR}/${dt}/${dt}.ready"  >> "${MOCK_DATA_SCRIPT}"
	echo " "  >> "${MOCK_DATA_SCRIPT}"
done


chmod +x ${SCRIPT_DIR}/generated/1-batch-mock-data.sh
###############

if [[ -f ${BATCH_RUN_SCRIPT} ]]; then
	rm -f ${BATCH_RUN_SCRIPT}
fi

echo "#!/bin/bash" > ${BATCH_RUN_SCRIPT}
echo "" >> ${BATCH_RUN_SCRIPT}
echo "" >> ${BATCH_RUN_SCRIPT}
echo "touch ${GEN_SCR_DIR}/running.lock" >> ${BATCH_RUN_SCRIPT}


run_args_base="PROFILE=${PROFILE}  APP_DEBUG=${APP_DEBUG} INPUT_DIR=${INPUT_DIR}  OUTPUT_DIR=${OUTPUT_DIR} WORK_DIR=${WORK_DIR}  LOOKBACK_DAYS=${LOOKBACK_DAYS} TABLES_FILE=${TABLES_FILE}"

echo "" >> ${BATCH_RUN_SCRIPT}

run_date=""
first_line="false"

for run_date in "${date_list[@]}"; do
	run_args=""

	if [[ "${FIRST_RUN}" == "true" ]]; then
		if [[ "$first_line" == "true" ]]; then
			run_args="$run_args_base SPRING_BATCH_INIT_SCHEMA=never"
		else
			run_args="$run_args_base SPRING_BATCH_INIT_SCHEMA=always"
			first_line="true"
		fi
	else
		run_args="$run_args_base SPRING_BATCH_INIT_SCHEMA=never"
	fi

	run_args="$run_args RUN_DATE=${run_date}"

	echo "$run_args ${SCRIPT_DIR}/start_dedup.sh" >> ${BATCH_RUN_SCRIPT}

	echo "" >> ${BATCH_RUN_SCRIPT}
done

echo "rm ${GEN_SCR_DIR}/running.lock" >> ${BATCH_RUN_SCRIPT}

chmod +x ${BATCH_RUN_SCRIPT}

exit $?
