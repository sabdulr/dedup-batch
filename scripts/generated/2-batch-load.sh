#!/bin/bash


touch /home/ubuntu/dedup-batch/scripts/generated/running.lock

PROFILE=prod  APP_DEBUG=false INPUT_DIR=/home/ubuntu/dedup-batch/data/input  OUTPUT_DIR=/home/ubuntu/dedup-batch/data/output WORK_DIR=/home/ubuntu/dedup-batch/data/db-temp/prod  LOOKBACK_DAYS=3 TABLES_FILE=/home/ubuntu/dedup-batch/config/tables.txt SPRING_BATCH_INIT_SCHEMA=never RUN_DATE=2026-03-20 /home/ubuntu/dedup-batch/scripts/start_dedup.sh

PROFILE=prod  APP_DEBUG=false INPUT_DIR=/home/ubuntu/dedup-batch/data/input  OUTPUT_DIR=/home/ubuntu/dedup-batch/data/output WORK_DIR=/home/ubuntu/dedup-batch/data/db-temp/prod  LOOKBACK_DAYS=3 TABLES_FILE=/home/ubuntu/dedup-batch/config/tables.txt SPRING_BATCH_INIT_SCHEMA=never RUN_DATE=2026-03-21 /home/ubuntu/dedup-batch/scripts/start_dedup.sh

PROFILE=prod  APP_DEBUG=false INPUT_DIR=/home/ubuntu/dedup-batch/data/input  OUTPUT_DIR=/home/ubuntu/dedup-batch/data/output WORK_DIR=/home/ubuntu/dedup-batch/data/db-temp/prod  LOOKBACK_DAYS=3 TABLES_FILE=/home/ubuntu/dedup-batch/config/tables.txt SPRING_BATCH_INIT_SCHEMA=never RUN_DATE=2026-03-22 /home/ubuntu/dedup-batch/scripts/start_dedup.sh

PROFILE=prod  APP_DEBUG=false INPUT_DIR=/home/ubuntu/dedup-batch/data/input  OUTPUT_DIR=/home/ubuntu/dedup-batch/data/output WORK_DIR=/home/ubuntu/dedup-batch/data/db-temp/prod  LOOKBACK_DAYS=3 TABLES_FILE=/home/ubuntu/dedup-batch/config/tables.txt SPRING_BATCH_INIT_SCHEMA=never RUN_DATE=2026-03-23 /home/ubuntu/dedup-batch/scripts/start_dedup.sh

PROFILE=prod  APP_DEBUG=false INPUT_DIR=/home/ubuntu/dedup-batch/data/input  OUTPUT_DIR=/home/ubuntu/dedup-batch/data/output WORK_DIR=/home/ubuntu/dedup-batch/data/db-temp/prod  LOOKBACK_DAYS=3 TABLES_FILE=/home/ubuntu/dedup-batch/config/tables.txt SPRING_BATCH_INIT_SCHEMA=never RUN_DATE=2026-03-24 /home/ubuntu/dedup-batch/scripts/start_dedup.sh

rm /home/ubuntu/dedup-batch/scripts/generated/running.lock
