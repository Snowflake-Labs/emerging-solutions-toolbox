from appPages.page import BasePage, set_page
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from PIL import Image
import re
from snowflake.snowpark.dataframe import DataFrame
import snowflake.snowpark.functions as F
from appUtil.constants import *
import base64
import io
from datetime import datetime, timedelta
import json
import time


def get_active_licenses():
    try:
        session = st.session_state.session
        active_sql = f"""
                               with
                                    combined_logs as (
                                    select app_id, session_user from {LICENSING_DB}.{LICENSING_SCHEMA}.{TBL_APP_LOGS}
                                    union
                                    select app_id, session_user from {LICENSING_DB}.{LICENSING_SCHEMA}.{TBL_OKTA_USERS}
                                    ),
                                    logs_with_metadata as (
                                        select ma.app_name, cl.app_id, cl.session_user, em.title, em.department, em.division
                                        from {LICENSING_DB}.{LICENSING_SCHEMA}.{TBL_EMPLOYEE_METADATA} em
                                        join combined_logs cl on (em.session_user = cl.session_user)
                                        join {LICENSING_DB}.{LICENSING_SCHEMA}.{TBL_MONITORED_APPS} ma on (cl.app_id = ma.app_id)
                                    )
                                select app_name, app_id, division, department, title, count(distinct session_user) as active_licenses from logs_with_metadata group by all
                            """
        active_licenses = session.sql(active_sql)
        # st.write(active_sql)
    except:
        active_licenses = pd.DataFrame()

    return active_licenses


def get_revocation_recommendations(app_id: int, run_id: str = None):
    session = st.session_state.session
    # st.write(f"{LICENSING_DB}.{LICENSING_SCHEMA}.{TBL_LICENSE_REVOCATION_RECOMMENDATION}")

    try:
        result = session.table(f"{LICENSING_DB}.{LICENSING_SCHEMA}.{TBL_LICENSE_REVOCATION_RECOMMENDATION}").filter((F.col("app_id") == F.lit(app_id)) & F.iff(F.lit(run_id).isNotNull(), F.col("run_id") == F.lit(run_id), F.lit(True))) \
            .select("*")

    except:
        result = DataFrame()

    return result


def run_model_today(app_id: int
                    , cutoff_days: int
                    , probability_no_login_revocation_threshold: float
                    , include_department: bool
                    , include_division: bool
                    , include_title: bool
                    , model_save: bool
                    ) -> dict:

    session = st.session_state.session

    with st.spinner('Running Model...'):
        # results = session.sql("CALL SNOWPATROL.MAIN.RUN_MODEL_TODAY(1,360,0.5,False,False,False,False)")
        results = session.sql("CALL " + LICENSING_DB + "." + LICENSING_SCHEMA + ".RUN_MODEL_TODAY(" + str(app_id) + "," + str(cutoff_days) + "," + str(
            probability_no_login_revocation_threshold) + "," + str(include_department) + "," + str(
            include_division) + "," + str(include_title) + "," + str(model_save) + ")").to_pandas()
        #results = session.sql("CALL LICENSING.MAIN.RUN_MODEL_TODAY(" + str(app_id) + "," + str(cutoff_days) + "," + str(probability_no_login_revocation_threshold) + "," + str(include_department) + "," + str(include_division) + "," + str(include_title) + "," + str(model_save) + ")").to_pandas()
        # results = session.call("MAIN.RUN_MODEL_TODAY", app_id, cutoff_days, probability_no_login_revocation_threshold,
        #                        include_department, include_division, include_title, model_save)

        return results


def create_download_link(val, page: int):
    b64 = base64.b64encode(val)  # val looks like b'...'
    # if page == 0:
    return f'**💾 ➡️ [Download](data:text/csv;base64,{b64.decode()})**'
    # else:
    # return f'**💾 ➡️ [Download Part {page+1}](data:text/csv;base64,{b64.decode()})**'


class GettingStartedPage(BasePage):
    def __init__(self):
        self.name = "getting_started"

    def print_page(self):
        session = st.session_state.session

        active_licenses = get_active_licenses()
        active_licenses = active_licenses.to_pandas()
        # st.dataframe(active_licenses)

        with st.container() as login_history_section:
            with st.container() as metrics_section:
                st.header("Login History ")
                metrics_section_col1, metrics_section_col2 = st.columns(2, gap="small")
                with metrics_section_col1:
                    al = 0 if active_licenses.empty else active_licenses['ACTIVE_LICENSES'].sum()
                    st.metric(label="**Total Active Licenses**", value=al)
                with metrics_section_col2:
                    apps_tracked = 0 if active_licenses.empty else active_licenses['APP_NAME'].nunique()
                    st.metric(label="**Applications Tracked**", value=apps_tracked)

            with st.container() as charts_section:
                if not active_licenses.empty:
                    st.header("License Usage")
                if not active_licenses.empty:
                    charts_section_col1, charts_section_col2 = st.columns(2, gap="small")
                    with charts_section_col1:
                        # """**License usage by Department**"""
                        _z = active_licenses.groupby(['APP_NAME', 'DEPARTMENT'])['ACTIVE_LICENSES'].sum().reset_index()
                        temp = _z.pivot(index='APP_NAME', columns='DEPARTMENT')['ACTIVE_LICENSES'].fillna(0)
                        fig_active_license_heatgrid = go.Figure(data=go.Heatmap(
                            z=temp.values,
                            x=temp.columns,
                            y=temp.index,
                            hoverongaps=True
                        ))
                        fig_active_license_heatgrid = fig_active_license_heatgrid.update_traces(
                            text=temp.values, texttemplate="%{text}",
                        )
                        fig_active_license_heatgrid.update_layout(title={
                            'text': "License usage by Department",
                            'font': {'size': 20},
                            'y': 0.9,
                            'x': 0.5,
                            'xanchor': 'center',
                            'yanchor': 'top'})
                        st.plotly_chart(fig_active_license_heatgrid, use_container_width=True, theme="streamlit")
                    with charts_section_col2:
                        _app_names = active_licenses['APP_NAME']
                        _values = active_licenses['ACTIVE_LICENSES']
                        plt = go.Figure(data=go.Pie(values=_values, labels=_app_names))
                        plt.update_layout(title={
                            'text': "License usage distribution by Apps",
                            'font': {'size': 20},
                            'y': 0.9,
                            'x': 0.45,
                            'xanchor': 'center',
                            'yanchor': 'top'})
                        plt.update_traces(textinfo='value')

                        st.plotly_chart(plt, use_container_width=True, theme="streamlit")

        with st.container() as revocation_recommendations:
            st.header("Revocation Recommendations")
            # st.dataframe(active_licenses)
            if not active_licenses.empty:
                app_list = active_licenses['APP_NAME'].unique()
                app_name = st.selectbox(label="**App Name**: Select an app ", options=app_list)

                app_id = int(active_licenses[active_licenses.APP_NAME == app_name].reset_index().at[0, 'APP_ID'])

                # st.write(app_id)
                runs_df = get_revocation_recommendations(app_id)

                runs_df = runs_df.to_pandas()
                # st.dataframe(runs_df)
                # st.write(runs_df.empty)
                run_list = runs_df['RUN_ID'].unique().tolist() if not runs_df.empty else []
                if len(run_list) > 0:
                    run_id = st.selectbox("Recent run id :- ", options=run_list, index=0)

                    # get_older_recommendations = st.button("Get", disabled=True if runs_df.empty else False)
                    get_older_recommendations = True
                    if get_older_recommendations:
                        response = {"status": "SUCCESS"}
                        recommendations_df = runs_df[runs_df.RUN_ID == run_id]
                        #st.dataframe(recommendations_df)
                        run_date = recommendations_df['TRAINING_DATE'].unique()[0]
                        probability_threshold = recommendations_df['THRESHOLD_PROBABILITY'].unique()[0]

                        _total_active = int(active_licenses[active_licenses.APP_ID == app_id]['ACTIVE_LICENSES'].sum())
                        _revocable = int(recommendations_df[recommendations_df.REVOKE == 1]["SESSION_USER"].nunique())
                        fig_active_vs_revocable = go.Figure(data=go.Pie(values=[(_total_active - _revocable), _revocable],
                                                                        labels=['ACTIVE', 'REVOCABLE'], hole=0.4)
                                                            )
                        fig_active_vs_revocable.update_traces(hoverinfo='label+value',
                                                              textinfo='percent', textfont_size=20,
                                                              marker=dict(colors=['gold', 'mediumturquoise'])
                                                              )
                        fig_active_vs_revocable.add_annotation(x=0.5, y=0.47,
                                                               text=f"{_revocable} / {_total_active}",
                                                               font=dict(size=15, family='Verdana',
                                                                         color='black'),
                                                               showarrow=False)

                        fig_active_vs_revocable.update_layout(title={
                            'text': "Active vs. Revocable Licenses",
                            'font': {'size': 20},
                            'y': 0.9,
                            'x': 0.45,
                            'xanchor': 'center',
                            'yanchor': 'top'})

                        st.plotly_chart(fig_active_vs_revocable, use_container_width=True, theme="streamlit")

                    with st.expander("Show me raw data "):
                        st.dataframe(recommendations_df)
                        # Convert Recomm. DF to CSV
                        recomm_output = io.StringIO()
                        recommendations_df.to_csv(recomm_output, index=False)

                        # Downloadable Link to CSV
                        data = create_download_link(recomm_output.getvalue().encode("utf-8"), 0)
                        st.markdown(data)

                        # st.download_button(label = "Downlaod data as CSV",
                        #                    data = recommendations_df.to_csv().encode('utf-8'),
                        #                    file_name="{}.csv".format("raw_data"),
                        #                    mime="text/csv", )

                with st.expander("**Retrain & get fresh recommendations:**"):
                    include_dept = False
                    include_div = False
                    include_title = False
                    save_model = False

                    cutoff_days = st.slider(
                        label="**Cutoff Days**: Number of days to go back from today to look for authentication patterns between the beginning and this cutoff date",
                        min_value=30, max_value=365, step=5, value=30)
                    run_date = datetime.now().date()
                    f"""(Cutoff Date): {run_date - timedelta(days=cutoff_days)}"""

                    probability_no_login_revocation_threshold = st.slider(
                        label="**Login probability threshold**: Predicted probabilities greater than or equal to threshold are recommended for revocations",
                        min_value=0.0, max_value=1.0, value=0.5)

                    with st.container() as get_recomm:

                        generate_new_recommendations = st.button("Generate")

                    if generate_new_recommendations:

                        # response={'status':''}
                        run_date = datetime.now().date()
                        probability_threshold = probability_no_login_revocation_threshold
                        # session_sdm.get_active_licenses()
                        # st.write(app_id,cutoff_days,probability_no_login_revocation_threshold,include_dept,include_div,include_title,save_model)
                        with st.spinner("Load Data..."):
                            time.sleep(0.01)

                        response = run_model_today(app_id,
                                                   cutoff_days,
                                                   probability_no_login_revocation_threshold,
                                                   include_dept,
                                                   include_div,
                                                   include_title,
                                                   save_model)

                        response_pd = pd.DataFrame(response)


                        obj_column = json.loads(response_pd['RUN_MODEL_TODAY'].values[0])
                        obj_run_id = obj_column['run_id']
                        obj_status = obj_column['status']


                        recommendations_df = get_revocation_recommendations(app_id, obj_run_id)
                        recommendations_df = recommendations_df.to_pandas()
                        if 'ERROR' in obj_status:
                            st.error(f"**{obj_status}**", icon="☠️")
                        else:
                            # st.progress()
                            st.success(f"**{obj_status}**", icon="👍")
                            # st.experimental_rerun()
                            st.divider()
                            with st.container() as recomm_results:

                                f"""**Run Date** : {run_date}"""
                                f"""**Login probability threshold** : {probability_threshold}"""

                                recomm_results_col0_spacer1, recomm_results_c1, recomm_results_col0_spacer2 = st.columns(
                                    (5, 25, 5))

                                with recomm_results_col0_spacer1:
                                    st.markdown("""""")

                                with recomm_results_c1:
                                    _total_active = int(
                                        active_licenses[active_licenses.APP_ID == app_id]['ACTIVE_LICENSES'].sum())
                                    _revocable = int(
                                        recommendations_df[recommendations_df.REVOKE == 1]["SESSION_USER"].nunique())
                                    fig_active_vs_revocable = go.Figure(
                                        data=go.Pie(values=[(_total_active - _revocable), _revocable],
                                                    labels=['ACTIVE', 'REVOCABLE'], hole=0.4)
                                    )
                                    fig_active_vs_revocable.update_traces(hoverinfo='label+value',
                                                                          textinfo='percent', textfont_size=20,
                                                                          marker=dict(
                                                                              colors=['gold', 'mediumturquoise'])
                                                                          )
                                    fig_active_vs_revocable.add_annotation(x=0.5, y=0.47,
                                                                           text=f"{_revocable} / {_total_active}",
                                                                           font=dict(size=15, family='Verdana',
                                                                                     color='black'),
                                                                           showarrow=False)

                                    fig_active_vs_revocable.update_layout(title={
                                        'text': "Active vs. Revocable Licenses",
                                        'font': {'size': 20},
                                        'y': 0.9,
                                        'x': 0.45,
                                        'xanchor': 'center',
                                        'yanchor': 'top'})

                                    st.plotly_chart(fig_active_vs_revocable, use_container_width=True,
                                                    theme="streamlit")

                            with recomm_results_col0_spacer2:
                                st.markdown("""""")

    def print_sidebar(self):
        super().print_sidebar()
