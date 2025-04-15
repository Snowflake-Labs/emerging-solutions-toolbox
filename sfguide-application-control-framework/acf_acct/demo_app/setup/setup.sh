#!/bin/sh
#script         :setup.sh
#create_date    :2024-03-01
#author         :Marc Henderson
#description    :This script uses SnowSQL to run the necessary scripts in the Provider's Account to
#                create the demo app objects.  NOTE:  optional, if the provider wants to create a sample 
#                app with the ACF.
#usage          :./setup.sh <relative_path_to_config_file>
#notes          :This should be ran once on Provider's account.

#Copyright © 2025 Snowflake Inc. All rights reserved

#==========================================================================================================
#summary of changes
#date(yyyy-mm-dd)     author                              comments
#----------------     -------------------------------     -------------------------------------------------
#2024-03-01           Marc Henderson                      Initial build
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

  #Demo app objects setup
  snowsql -c $ACF_ACCOUNT_LOCATOR \
          -f sql/setup.sql \
          -D DIR=$DIR \
          -D ACF_ACCOUNT_LOCATOR=$ACF_ACCOUNT_LOCATOR \
          -D APP_CODE=$APP_CODE \
          -o output_format=plain \
          -o header=False
fi