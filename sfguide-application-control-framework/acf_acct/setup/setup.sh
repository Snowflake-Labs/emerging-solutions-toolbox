#!/bin/sh
#script         :setup.sh
#create_date    :2022-04-06
#author         :Marc Henderson
#description    :This script uses SnowSQL to run the necessary scripts in the Provider's Account to
#                create the ACF objects.
#usage          :./setup.sh <relative_path_to_config_file>
#notes          :This should be ran once on Provider's account.

#Copyright © 2025 Snowflake Inc. All rights reserved

#==========================================================================================================
#summary of changes
#date(yyyy-mm-dd)     author                              comments
#----------------     -------------------------------     -------------------------------------------------
#2022-03-14           Marc Henderson                      Initial build
#2023-05-01           Marc Henderson                      Removed unused parameters
#2023-06-28           Marc Henderson                      Removed SOURCE_TABLE_LIST, added DIR
#2023-10-09           Marc Henderson                      Renamed ACCOUNT_LOCATOR to ACF_ACCOUNT_LOCATOR
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
            ACF_ACCOUNT_LOCATOR)
                ACF_ACCOUNT_LOCATOR=$value
                printf '%s\n' "${parameter} = ${ACF_ACCOUNT_LOCATOR}"
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
  snowsql -c $ACF_ACCOUNT_LOCATOR \
          -f sql/setup.sql \
          -D DIR=$DIR \
          -D ACF_ACCOUNT_LOCATOR=$ACF_ACCOUNT_LOCATOR \
          -D APP_CODE=$APP_CODE \
          -o output_format=plain \
          -o header=False
fi