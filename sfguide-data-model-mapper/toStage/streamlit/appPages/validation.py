import streamlit as st
import pandas as pd
from appPages.page import BasePage, col, set_page


class ValidationPage(BasePage):
    def __init__(self):
        self.name = "validation"

    def print_page(self):
        session = st.session_state.session
        st.title("Validation")
        st.write("")
        view_name = f'{st.session_state.selected_target_collection}__v1__{st.session_state.collection_entity_name}'
        call_sp = f"""call VALIDATION.VALIDATE('{view_name}')"""

        check_if_validations_exist_sql = (f"SELECT * FROM data_model_mapper_app_package.ADMIN.VALIDATION_RULE WHERE "
                                          f"TARGET_TABLE = '{view_name}'")
        check_if_validations_exist = session.sql(check_if_validations_exist_sql).collect()
        if len(check_if_validations_exist) > 0:

            try:
                session.sql(call_sp).collect()
            except Exception as error:
                st.error(f"An exception occurred: {str(error)}")

            validation_results = f"""WITH closest_timestamp AS (select run_ts from data_model_mapper_share_db.validated.validation_log order by abs(datediff(second, run_ts, current_timestamp)) ASC limit 1) select validation_result, run_ts, rule_name, target_table, column_names, validation_type, validation_descrip, validation_msg, validation_num_passed, validation_num_failed, validation_num_passed + validation_num_failed as total_validations from data_model_mapper_share_db.validated.validation_log where run_ts = (select run_ts from closest_timestamp) and target_table = '{view_name}';"""
            results_df = session.sql(validation_results).collect()

            if len(results_df) > 0:
                validation_results_df = pd.DataFrame(results_df)
                metrics_df = validation_results_df[
                    ['TOTAL_VALIDATIONS', 'VALIDATION_NUM_PASSED', 'VALIDATION_NUM_FAILED', 'RULE_NAME']]
                validation_display_df = validation_results_df[
                    ['VALIDATION_RESULT', 'RUN_TS', 'TARGET_TABLE', 'COLUMN_NAMES', 'VALIDATION_TYPE', 'VALIDATION_DESCRIP',
                     'VALIDATION_MSG']]
                total_validations = metrics_df['TOTAL_VALIDATIONS'].sum()
                validations_passed = metrics_df['VALIDATION_NUM_PASSED'].sum()
                validations_failed = metrics_df['VALIDATION_NUM_FAILED'].sum()

                validations, passed, failed = st.columns((1, 1, 1))
                with validations:
                    st.write("Total Validations")
                    st.subheader(total_validations)
                with passed:
                    st.write("Total Validations Passed")
                    st.subheader(validations_passed)
                with failed:
                    st.write("Total Validations Failed")
                    st.subheader(validations_failed)

                st.write("")
                st.dataframe(validation_display_df)
                st.write("")

                new_metrics_df = metrics_df.set_index("RULE_NAME")
                chart_data = pd.DataFrame(new_metrics_df, columns=["VALIDATION_NUM_PASSED", "VALIDATION_NUM_FAILED"])
                st.bar_chart(chart_data, x_label="Validation Type", y_label="Num Passed/Failed",
                             color=["#DC143C", "#00A36C"])

            else:
                st.warning(
                    'Your data mapping has not been validated. Please go to the Target Collections page and click on the Configure button to delete your data mapping. After you have done that, please start the process over. It is also possible the target table you are trying to map to does not exist in the DATA_MODEL_MAPPER_APP.VALIDATION.VALIDATION_RULE view.')
        else:
            st.info('There are currently no extra (optional) validations for this Target Collection. You may return to the target collections list by clicking the Collections list button below')
        col1, col2, col3 = st.columns((1, 2.5, 1))
        with col1:
            st.button(
                "View Validations History",
                key="view_validations_history",
                on_click=set_page,
                args=("validations_history",),
            )
        with col2:
            st.write("")
        with col3:
            st.button(
                "Return to Collections",
                key="return_to_collections",
                on_click=set_page,
                type="primary",
                args=("collection_list",),
            )

    def print_sidebar(self):
        super().print_sidebar()
