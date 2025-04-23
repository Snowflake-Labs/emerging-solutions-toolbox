APP_OPP_DB = "DATA_QUALITY"
APP_CONFIG_SCHEMA = "CONFIG"
APP_RESULTS_SCHEMA = "RESULTS"
APP_DATA_SCHEMA = "DATA"
APP_TEMP_DATA_SCHEMA = "TEMPORARY_DQ_OBJECTS"
dates_chron_dict = {
            "Hourly": "0 * * * *",
            "Daily":"0 1 * * *", 
            "Weekly": "0 1 * * 1", 
            "Monthly":"0 1 1 * *",
            "Annually":"0 1 1 1 *"
        }
reverse_chron_dict = inv_map = {v: k for k, v in dates_chron_dict.items()}
