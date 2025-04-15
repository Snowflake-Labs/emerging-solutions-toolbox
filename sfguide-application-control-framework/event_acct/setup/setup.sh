#!/bin/sh
#script         :setup.sh
#create_date    :2022-04-06
#author         :Marc Henderson
#description    :This script uses SnowSQL to run the necessary scripts in the Provider's Event Account to
#                create the event table.
#usage          :./setup.sh <relative_path_to_config_file>
#notes          :This should be ran once on Provider's account.

#Copyright © 2025 Snowflake Inc. All rights reserved

#==========================================================================================================
#summary of changes
#date(yyyy-mm-dd)     author                              comments
#----------------     -------------------------------     -------------------------------------------------
#2022-10-09           Marc Henderson                      Initial build
#==========================================================================================================

file=$1
if [ -z "${file}" ]; then
  # if no config file passed in, show message and exit
  echo "Please enter the path to a configuration file as a parameter (ex ./setup.sh config.txt)"
else
  printf "\nConfig Parameters:\n"
  # if config file passed in, collect variables via config file
    while read -r line || [ -n "$line" ]
    do
        parameter=$(echo $line | cut -d'=' -f 1)
        value=$(echo $line | cut -d'=' -f 2)
        case $parameter in
            DIR)
                DIR=$value
                printf '%s\n' "${parameter} = ${DIR}"
                ;;
            ORG_NAME)
                ORG_NAME=$value
                printf '%s\n' "${parameter} = ${ORG_NAME}"
                ;;
            ACF_ACCOUNT_LOCATOR)
                ACF_ACCOUNT_LOCATOR=$value
                printf '%s\n' "${parameter} = ${ACF_ACCOUNT_LOCATOR}"
                ;;
            ACF_ACCOUNT_NAME)
                ACF_ACCOUNT_NAME=$value
                printf '%s\n' "${parameter} = ${ACF_ACCOUNT_NAME}"
                ;;
            EVENT_ACCOUNT_LOCATOR)
                EVENT_ACCOUNT_LOCATOR=$value
                printf '%s\n' "${parameter} = ${EVENT_ACCOUNT_LOCATOR}"
                ;;  
            APP_CODE)
                APP_CODE=$value
                printf '%s\n' "${parameter} = ${APP_CODE}"
                ;;
        esac
    done < "$file"

  printf "\n"

  #change directories to the current script
  cd "${0%/*}"

  #Native App setup
  snowsql -c $EVENT_ACCOUNT_LOCATOR \
          -f sql/setup.sql \
          -D DIR=$DIR \
          -D ORG_NAME=$ORG_NAME \
          -D ACF_ACCOUNT_LOCATOR=$ACF_ACCOUNT_LOCATOR \
          -D ACF_ACCOUNT_NAME=$ACF_ACCOUNT_NAME \
          -D EVENT_ACCOUNT_LOCATOR=$EVENT_ACCOUNT_LOCATOR \
          -D APP_CODE=$APP_CODE \
          -o output_format=plain \
          -o header=False
fi