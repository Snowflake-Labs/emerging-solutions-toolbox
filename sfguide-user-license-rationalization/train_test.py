from snowflake.snowpark import Session
import snowflake.snowpark.functions as F
import snowflake.snowpark.types as T
from snowflake.snowpark import Window
import sys
from appUtil.constants import *
import pandas as pd

from datetime import datetime, timedelta

# Comment tag for Snowflake object tagging
comment_tag = """comment='{"origin":"sf_sit","name":"user_license_rationalization","version":{"major":1, "minor":0},"attributes":{"component":"streamlit"}}'"""


def contains_anyof(str_to_match: str, list_of_values: list) -> bool:
    return any(v.lower() in str_to_match.lower() for v in list_of_values)


def create_license_usage_prediction_udf(session: Session, udf_name: str, model, predictor_cols, stage_loc, py_packages,
                                        custom_packages=[], probability_no_login_revocation_threshold=0.5):
    input_df_type = T.PandasDataFrameType([T.FloatType() for _ in predictor_cols])
    return_df_type = T.PandasSeriesType(T.FloatType())

    @F.udf(name=udf_name, is_permanent=True, stage_location=stage_loc, max_batch_size=1000, packages=py_packages,
           imports=custom_packages, replace=True, session=session
        , return_type=return_df_type, input_types=[input_df_type])
    def udf_predict_login_probability(df):
        # The input pandas DataFrame doesn't include column names. Specify the column names explicitly when needed.
        df.columns = predictor_cols
        probability_no_login = model.predict_proba(df[predictor_cols])[:, 1]
        return probability_no_login
    
    # Add comment tag to the UDF
    session.sql(f"ALTER FUNCTION {udf_name}(ARRAY) SET {comment_tag}").collect()


# Inspired by def binarize_and_stuff_data(...) in updated_obtain_revication_candidates.py
def binarize_and_stuff_data(auth_logs, work_days, employee_metadata, whitelisted_users, cutoff_days,target_variable_no_login):
    # Filter employees not having any of the elevated job titles
    employee_metadata_1 = employee_metadata.filter(~F.call_udf(f"{LICENSING_DB}.{LICENSING_SCHEMA}.udf_contains_anyof", F.col("title"),F.lit(TITLES_KW_WITH_ACCESS_RETAINED)))

    # Filter authentication logs with users not in global white list
    # filtered_logs_1 = auth_logs.filter(~F.lower(F.col("session_user")).in_([F.lit(val.lower()) for val in GLOBAL_WHITELIST]))
    filtered_logs_1 = auth_logs.join(whitelisted_users, on=(F.lower(auth_logs.session_user) == F.lower(whitelisted_users.email)), how="anti")

    # Filter authentication logs until cutoff date
    today = datetime.now().date()
    max_date = today - timedelta(days=cutoff_days)

    print("today and max day", today,max_date)

    filtered_logs_2 = filtered_logs_1.filter(F.col('authentication_datetime') <= F.lit(max_date))
    work_days_1 = work_days.filter(F.col('snapshot_datetime') <= F.lit(max_date))

    # Filter authentication logs to drop ex-employees
    offset_days = cutoff_days + target_variable_no_login
    ex_emp_cutoff_date = today - timedelta(days=offset_days)

    # window1 = Window.partition_by("session_user")

    filtered_logs_3 = filtered_logs_2.join(employee_metadata_1,on=(F.lower(filtered_logs_2.session_user) == F.lower(employee_metadata_1.session_user)), how="inner", lsuffix="_l",rsuffix="_r") \
        .filter(~(F.col("last_day_of_work").isNotNull() & (F.col("last_day_of_work") < F.lit(ex_emp_cutoff_date)))) \
        .with_column_renamed("session_user_l", "SESSION_USER")

    logins_per_user_per_day = filtered_logs_3.select(F.col("session_user"), F.col("authentication_datetime"),
                                                     F.to_date(F.col("authentication_datetime")).alias(
                                                         "authentication_date"), F.col("title"),
                                                     F.col("division"), F.col("department")) \
        .group_by(F.col("session_user"), F.col("authentication_date")) \
        .agg(
        F.col("session_user")
        , F.col('authentication_date')
        , F.count('authentication_datetime').alias('num_of_logins')
        , F.max('title').alias('title')
        , F.max('division').alias('division')
        , F.max('department').alias('department')
    )

    # Now the dataframe with each user and authentication date is mixed with the dataframe with work-day/holiday info.
    all_workdays_with_session_user_1 = work_days_1.cross_join(filtered_logs_3) \
        .group_by(F.col("snapshot_datetime"), F.col("session_user")).agg(F.col("snapshot_datetime"),
                                                                         F.col("session_user"),
                                                                         F.max("work_day").alias("work_day"))

    all_workdays_with_session_user_2 = all_workdays_with_session_user_1.join(logins_per_user_per_day, how="leftouter"
                                                                             , on=((all_workdays_with_session_user_1[
                                                                                        'snapshot_datetime'] ==
                                                                                    logins_per_user_per_day[
                                                                                        'authentication_date'])
                                                                                   &
                                                                                   (all_workdays_with_session_user_1[
                                                                                        'session_user'] ==
                                                                                    logins_per_user_per_day[
                                                                                        'session_user'])
                                                                                   )
                                                                             , lsuffix="_l"
                                                                             , rsuffix="_r") \
        .with_column_renamed("session_user_l", "session_user") \
        .with_column("authenticated", F.iff(F.col("num_of_logins").isNull(), 0, 1))

    # spdf_days_users_with_work_1 = work_days_1.join(filtered_logs_3, how="leftouter",on=(work_days_1['snapshot_datetime'] == F.to_date(filtered_logs_3['authentication_datetime'])) )
    # # A user is considered authenticated if a matching snapshot_datetime was found for the authentication_datetime
    # spdf_days_users_with_work_2 = spdf_days_users_with_work_1.with_column("authenticated", F.iff(F.col('authentication_datetime').isNotNull(), 1, 0))

    all_workdays_with_session_user_3 = all_workdays_with_session_user_2.select(
        F.col('session_user')
        , F.col('snapshot_datetime')
        # ,F.col('authentication_datetime')
        , F.col('work_day')
        , F.col('authenticated')
        , F.col('title')
        , F.col('division')
        , F.col('department')
    )

    return all_workdays_with_session_user_3


# Inspired by def obtain_revocation_candidates(...) in updated_obtain_revication_candidates.py
def generate_feature_data_set(app_id:int
                              ,auth_logs
                              ,work_days
                              ,employee_metadata
                              ,whitelisted_users
                              ,cutoff_days
                              ,target_variable_days_no_login
                              ,half_life_variable
                              ,grace_period_before_training
                           ):

    bin_data = binarize_and_stuff_data(auth_logs, work_days, employee_metadata, whitelisted_users, cutoff_days=0, target_variable_no_login = target_variable_days_no_login)

    today = datetime.now().date()
    cutoff_date = today - timedelta(days = cutoff_days)
    # end_date = cutoff_date - timedelta(days=target_variable_days_no_login)

    logins_before_cutoff = bin_data.filter(F.col("snapshot_datetime") <= F.to_date(F.lit(cutoff_date)))
    window1 = Window.partition_by("session_user")
    logins_before_cutoff_1 = logins_before_cutoff\
               .select(
                        F.col("session_user")
                        , F.col("title")
                        , F.col("department")
                        , F.col("division")
                        , F.col("snapshot_datetime")
                        , F.col("authenticated")
                        , F.col("work_day")
                     )\
    .with_column("days_before_cutoff", F.datediff("days",F.col("snapshot_datetime"), F.lit(cutoff_date)))\
    .with_column("weight",  F.pow(F.lit(2),F.lit(-1)*(F.col("days_before_cutoff")/F.lit(half_life_variable))))\
    .with_column("weighted_authentication", F.col("weight")*F.col("authenticated"))\
    .with_column("weighted_authentication_eligible", F.col("weight")*F.col("work_day") )\
    .with_column("first_authentication",F.min(F.iff(F.col("authenticated")==1,F.col("snapshot_datetime"),None)).over(window1))\
    .with_column("most_recent_authentication", F.max(F.iff(F.col("authenticated")==1, F.col("snapshot_datetime"), None)).over(window1))

    logins_before_cutoff_2 = logins_before_cutoff_1.filter(F.col("first_authentication") < (F.date_add(F.lit(cutoff_date),-1*F.lit(grace_period_before_training))))
    # Aggregate the data per user
    logins_before_cutoff_3 = logins_before_cutoff_2.group_by(F.col("session_user"))\
                              .agg(
                                [ F.max(F.col("days_before_cutoff")).alias("max_days_before_cutoff")
                                 ,F.count_distinct(F.iff(F.col("authenticated")==1,F.col("snapshot_datetime"),None)).alias("authentications")
                                 ,F.max(F.col("title")).alias("title")
                                 ,F.max(F.col("department")).alias("department")
                                 ,F.max(F.col("division")).alias("division")
                                 ,F.sum(F.col("weighted_authentication")).alias("sum_weighted_authentication")
                                 ,F.sum(F.col("weighted_authentication_eligible")).alias("sum_weighted_authentication_eligible")
                                 ,F.sum(F.iff(F.col("snapshot_datetime") > F.col("most_recent_authentication"), F.col("work_day"),0)).alias("work_days_since_last_login")
                                 ]
                              )\
                              .with_column("log_work_days_since_last_login", F.sql_expr("ln(work_days_since_last_login+1)"))\
                              .with_column("weighted_authentications_per_day", F.iff(F.col("sum_weighted_authentication_eligible") < 1 ,F.col("sum_weighted_authentication"),F.col("sum_weighted_authentication")/F.col("sum_weighted_authentication_eligible")))\
                              .with_column("authentications_per_day",(F.col("authentications")/F.col("max_days_before_cutoff")))\
                              .with_column("training_date", F.lit(today))\
                              .with_column("cutoff_date", F.lit(cutoff_date))\
                              .with_column("fold", F.lit(cutoff_days))

    logins_after_cutoff = bin_data.filter(F.col("snapshot_datetime") > F.to_date(F.lit(cutoff_date)))

    logins_after_cutoff_1 = logins_after_cutoff.select(F.col("session_user"), F.col("snapshot_datetime"), F.col("authenticated"), F.col("work_day")) \
        .with_column("days_until_next_login", F.iff(F.col("authenticated") == 1, F.datediff("days", F.lit(cutoff_date), F.col("snapshot_datetime")), None))

    # Aggregate the data per user
    logins_after_cutoff_2 = logins_after_cutoff_1.group_by(F.col("session_user")) \
        .agg([F.max(F.col("days_until_next_login")).alias("days_until_next_login")
    ]) \
        .with_column("did_not_login", F.iff(F.col("days_until_next_login") <= F.lit(target_variable_days_no_login), 0, 1))

    user_kpis = logins_before_cutoff_3.join(logins_after_cutoff_2, on=["session_user"], how="leftouter", lsuffix="_l", rsuffix="_r") \
        .select(
        F.lit(app_id).alias("app_id")
        , F.col("session_user")
        # ,F.col("authentication_datetime_l").alias("authentication_datetime")
        , F.col("title")
        , F.col("department")
        , F.col("division")
        # ,F.col("most_recent_authentication")
        , F.col("weighted_authentications_per_day")
        , F.col("training_date")
        , F.col("cutoff_date")
        , F.col("work_days_since_last_login")
        , F.col("log_work_days_since_last_login")
        , F.col("authentications_per_day")
        , F.col("days_until_next_login")
        , F.col("did_not_login")
        , F.col("fold")
    ).na.fill({
        "did_not_login": 1,  # default to no login if no matching record found in the dataset greater than cutoff date
        "work_days_since_last_login": 0
    })

    return user_kpis


def run_model_today( app_id: int
                    , cutoff_days: int
                    , probability_no_login_revocation_threshold: float
                    , include_department: bool
                    , include_division: bool
                    , include_title: bool
                    , model_save: bool
                    ) -> dict:
    from sklearn.linear_model import LogisticRegression
    import os
    import joblib
    import numpy as np
    import logging
    import uuid
    import json
    import pandas as pd

    session = set_session()

    # Logging only ERROR or higher in severity logs from this procedure
    logging.basicConfig(stream=sys.stdout, level=logging.ERROR)
    proc_logger = logging.getLogger("train_license_revocation_model")

    exec_status = {
        "status": "INFO: Starting the model training & recommendation generation",
        "run_id": None,
        "target_table": None
    }

    # Create Snowpark DataFrames for the referenced source data
    okta_logs = session.table(f"{LICENSING_DB}.{LICENSING_SCHEMA}.{TBL_OKTA_USERS}") \
        .select(
        F.col('"SESSION_USER"').alias('session_user')
        , F.to_date(F.col('"SNAPSHOT_DATETIME"')).alias('authentication_datetime')
        , F.col('"APP_ID"').alias('app_id')
    )



    okta_logs.show()

    app_logs = session.table(f"{LICENSING_DB}.{LICENSING_SCHEMA}.{TBL_APP_LOGS}") \
        .select(
        F.col('"SESSION_USER"').alias('session_user')
        , F.to_date(F.col('"SNAPSHOT_DATETIME"')).alias('authentication_datetime')
        , F.col('"APP_ID"').alias('app_id')
    )

    app_logs.show()

    work_days = session.table(f"{LICENSING_DB}.{LICENSING_SCHEMA}.{TBL_WORK_DAYS}") \
        .select(
        F.iff(F.col('"WORK_DAY"'), 1, 0).alias('work_day')
        , F.col('"SNAPSHOT_DATETIME"').alias('snapshot_datetime')
    )
    work_days.show()

    employee_metadata = session.table(f"{LICENSING_DB}.{LICENSING_SCHEMA}.{TBL_EMPLOYEE_METADATA}") \
        .select(F.col('"APP_ID"').alias('app_id')
                , F.col('"SESSION_USER"').alias('session_user')
                , F.col('"DEPARTMENT"').alias('department')
                , F.col('"DIVISION"').alias('division')
                , F.col('"TITLE"').alias('title')
                , F.col('"LAST_DAY_OF_WORK"').alias('last_day_of_work')
                ) \
        .filter(F.col('app_id') == app_id)

    employee_metadata.show()

    whitelisted_users = session.table(f"{LICENSING_DB}.{LICENSING_SCHEMA}.{TBL_WHITELISTED_USERS}").select(F.col('"EMAIL"').alias("email"))
    whitelisted_users.show()

    # Convert "Snapshot datetime" to simple date and drop duplicates for both data sets.
    unique_okta_logs = okta_logs.filter(F.col('app_id') == app_id).select(F.col('session_user'), F.col('authentication_datetime')).distinct()
    unique_app_logs = app_logs.filter(F.col('app_id') == app_id).select(F.col('session_user'), F.col('authentication_datetime')).distinct()
    combined_auth_logs_df = unique_okta_logs.union(unique_app_logs)
    combined_auth_logs_df.show()

    this_run_id = str(uuid.uuid1())
    train_user_kpis = generate_feature_data_set(
        app_id
        , combined_auth_logs_df
        , work_days
        , employee_metadata
        , whitelisted_users
        , cutoff_days=cutoff_days
        # Cutoff days allows us to create more folds into the past, counting back from current-date
        , target_variable_days_no_login=365
        , half_life_variable=30
        , grace_period_before_training=30
    ).to_pandas()

    print(train_user_kpis.to_string())
    train_user_kpis.columns = train_user_kpis.columns.str.upper()
    train_user_kpis = train_user_kpis.assign(run_id=this_run_id)
    session.write_pandas(train_user_kpis, f"{TBL_LICENSE_USAGE_FEATURES}", database=f"{LICENSING_DB}", schema=f"{LICENSING_SCHEMA}", auto_create_table=True)
    session.sql(f"ALTER TABLE {LICENSING_DB}.{LICENSING_SCHEMA}.{TBL_LICENSE_USAGE_FEATURES} SET {comment_tag}").collect()

    test_user_kpis = generate_feature_data_set(app_id
                                               , combined_auth_logs_df
                                               , work_days
                                               , employee_metadata
                                               , whitelisted_users
                                               , cutoff_days=0
                                               # Cutoff days allows us to create more folds into the past, counting back from current-date
                                               , target_variable_days_no_login=30
                                               , half_life_variable=30
                                               , grace_period_before_training=30).to_pandas()
    print("test_user_kpi's",test_user_kpis.to_string())
    # Train model now over the full set of user-kpis for training
    try:
        predictor_columns = ['AUTHENTICATIONS_PER_DAY', 'WEIGHTED_AUTHENTICATIONS_PER_DAY', 'WORK_DAYS_SINCE_LAST_LOGIN', 'LOG_WORK_DAYS_SINCE_LAST_LOGIN']

        train_predictor_variables = train_user_kpis.loc[:, predictor_columns].reset_index(drop=True)
        target_variables = train_user_kpis['DID_NOT_LOGIN']
        # target_variables.columns = ['did_not_login']
        test_predictor_variables = test_user_kpis.loc[:, predictor_columns].reset_index(drop=True)
        print(test_predictor_variables.to_string())
        print("target_variables")
        print(target_variables.to_string())
        print(include_department)
    #     if include_department:
    #         train_predictor_variables = pd.concat([
    #             train_predictor_variables
    #             , pd.get_dummies(
    #                 train_user_kpis.assign(department=np.where(pd.isna(train_user_kpis.DEPARTMENT), 'none',
    #                                                            train_user_kpis.DEPARTMENT.str.lower().str.replace(' ',
    #                                                                                                               '_')))['department']).reset_index(drop=True)
    #         ], axis=1)
    #
    #         test_predictor_variables = pd.concat([
    #             test_predictor_variables
    #             , pd.get_dummies(
    #                 test_user_kpis.assign(department=np.where(pd.isna(test_user_kpis.DEPARTMENT), 'none',
    #                                                           test_user_kpis.DEPARTMENT.str.lower().str.replace(' ',
    #                                                                                                             '_')))['department']).reset_index(drop=True)
    #         ], axis=1)
    #
    #     if include_division:
    #         train_predictor_variables = pd.concat([
    #             train_predictor_variables
    #             , pd.get_dummies(
    #                 train_user_kpis.assign(division=np.where(pd.isna(train_user_kpis.DIVISION), 'none',
    #                                                          train_user_kpis.DIVISION.str.lower().str.replace(' ',
    #                                                                                                           '_')))['division']).reset_index(drop=True)
    #         ], axis=1)
    #
    #         test_predictor_variables = pd.concat([
    #             test_predictor_variables
    #             , pd.get_dummies(
    #                 test_user_kpis.assign(division=np.where(pd.isna(test_user_kpis.DIVISION), 'none',
    #                                                         test_user_kpis.DIVISION.str.lower().str.replace(' ', '_')))['division']).reset_index(drop=True)
    #         ], axis=1)
    #
    #     if include_title:
    #         train_predictor_variables = pd.concat([
    #             train_predictor_variables
    #             , pd.get_dummies(
    #                 train_user_kpis.assign(title=np.where(pd.isna(train_user_kpis.TITLE), 'none',
    #                                                       train_user_kpis.TITLE.str.lower().str.replace(' ', '_')))['title']).reset_index(drop=True)
    #         ], axis=1)
    #
    #         test_predictor_variables = pd.concat([
    #             test_predictor_variables
    #             , pd.get_dummies(
    #                 test_user_kpis.assign(title=np.where(pd.isna(test_user_kpis.TITLE), 'none',
    #                                                      test_user_kpis.TITLE.str.lower().str.replace(' ', '_')))['title']).reset_index(drop=True)
    #         ], axis=1)
    #
        if train_predictor_variables.shape[0] > 0:
            model = LogisticRegression()
            trained_logistic_regression_model = model.fit(train_predictor_variables, target_variables)

            if model_save:
                # Upload trained model to a stage
                now_ts = str(datetime.now().timestamp())
                model_name = f'model_{now_ts}.joblib'
                model_file = os.path.join('/tmp', model_name)

                joblib.dump(trained_logistic_regression_model, model_file)
                session.file.put(model_file, f"@{LICENSING_DB}.{LICENSING_SCHEMA}.{MODELS_STAGE}", overwrite=True, auto_compress=False)
                exec_status = {
                    "status": f"INFO: Model trained as @{LICENSING_DB}.{LICENSING_SCHEMA}.{MODELS_STAGE}/{model_name}",
                    "run_id": None,
                    "target_table": None
                }

                # register a udf that can then call this model for predictions
                create_license_usage_prediction_udf(
                    session
                    , udf_name=f"{LICENSING_DB}.{LICENSING_SCHEMA}.udf_predict_login_probability"
                    , model=trained_logistic_regression_model
                    , predictor_cols=predictor_columns
                    , stage_loc=f"@{LICENSING_DB}.{LICENSING_SCHEMA}.{OBJECT_STAGE}"
                    , py_packages=['snowflake-snowpark-python', 'pandas', 'numpy', 'scikit-learn==1.2.1']
                )

            test_user_kpis = test_user_kpis.assign(threshold_probability=probability_no_login_revocation_threshold)
            test_user_kpis = test_user_kpis.assign(probability_no_login=trained_logistic_regression_model.predict_proba(test_predictor_variables)[:, 1])
            test_user_kpis = test_user_kpis.assign(revoke=np.where(test_user_kpis.probability_no_login > probability_no_login_revocation_threshold, 1, 0))
            test_user_kpis = test_user_kpis.assign(run_id=this_run_id)

            # Create a Snowpark DF out of the final Pandas DF and write to Snowflake table
            test_user_kpis.columns = test_user_kpis.columns.str.upper()
            session.write_pandas(test_user_kpis, f"{TBL_LICENSE_REVOCATION_RECOMMENDATION}", database=f"{LICENSING_DB}", schema=f"{LICENSING_SCHEMA}", auto_create_table=True, overwrite=False)
            session.sql(f"ALTER TABLE {LICENSING_DB}.{LICENSING_SCHEMA}.{TBL_LICENSE_REVOCATION_RECOMMENDATION} SET {comment_tag}").collect()

            exec_status = {
                "status": "SUCCESS",
                "run_id": this_run_id,
                "target_table": f"{LICENSING_DB}.{LICENSING_SCHEMA}.{TBL_LICENSE_REVOCATION_RECOMMENDATION}"
            }
            # license_revocation_recommendation = session.create_dataframe(test_user_kpis)
            # license_revocation_recommendation.write.mode("overwrite").save_as_table(f"{LICENSING_DB}.{LICENSING_SCHEMA}.license_revocation_recommendation")
        else:
            exec_status = {
                "status": "ERROR",
                "run_id": None,
                "target_table": None
            }
    except Exception as e:
        exec_status = {
            "status": f"ERROR: Could not train the model.\nCaught exception {e}",
            "run_id": None,
            "target_table": None
        }

        proc_logger.error(json.dumps(exec_status))

    return exec_status

import streamlit as st
from snowflake.snowpark.context import get_active_session
from appPages.start import StartPage
from appPages.getting_started import GettingStartedPage


def set_session():

    import snowflake_conn as sfc

    session = sfc.init_snowpark_session("account")

    st.session_state["streamlit_mode"] = "OSS"

    return session


def main():
    st.set_page_config(layout="wide")
    if 'session' not in st.session_state:
        st.session_state.session = set_session()

    print("working")

    run_model_today(1, 120, 0.5, False, False, False, False)


main()