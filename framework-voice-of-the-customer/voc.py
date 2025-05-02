import streamlit as st
from snowflake.snowpark import functions as F
from snowflake.snowpark import Session
from snowflake.snowpark import types as T
from page import BasePage, pd

SUMMARIZE_PROMPT = (
    "[SYSTEM] You're an expert summarizer. Summarize the following transcript while retaining the context. "
    "[USER] Do include include **Summary** or a prefix in the response."
)
TOPIC_PROMPT = (
    "[SYSTEM] You are assigned to review various conversations and text snippets. Be brief and to the point. Do not provide additional reasoning in the response. "
    "[USER] What are the main topics discussed by the customers? Do not respond with any preamble. Your answer should be in an array format. Do not include any special characters or backticks."
)


def fetch_databases(session: Session):
    databases = session.sql("SHOW TERSE DATABASES").collect()
    database_name = [d["name"].lower() for d in databases]
    return database_name


def fetch_schemas(session: Session, database_name):
    schemas = session.sql(f"SHOW TERSE SCHEMAS IN {database_name}").collect()
    schema_name = [s["name"].lower() for s in schemas]
    schema_pd = pd.DataFrame(schema_name)
    schema_name = schema_pd[schema_pd[0] != "information_schema"][0].tolist()
    return schema_name


def fetch_tables(session: Session, database_name, schema_name):
    tables = session.sql(
        f"SHOW TERSE TABLES IN {database_name}.{schema_name}"
    ).collect()
    table_name = [t["name"].lower() for t in tables]
    return table_name


def fetch_views(session: Session, database_name, schema_name):
    views = session.sql(f"SHOW TERSE VIEWS IN {database_name}.{schema_name}").collect()
    view_name = [v["name"] for v in views]
    return view_name


def fetch_streams(session: Session, database_name, schema_name):
    streams = session.sql(
        f"SHOW TERSE STREAMS IN {database_name}.{schema_name}"
    ).collect()
    stream_name = [v["name"] for v in streams]
    return stream_name


def check_fully_qualified():
    if (
        st.session_state.selected_database is not None
        and st.session_state.selected_schema is not None
        and st.session_state.selected_table is not None
    ):
        st.session_state.qualified_selected_table = (
            st.session_state.selected_database
            + "."
            + st.session_state.selected_schema
            + "."
            + st.session_state.selected_table
        )
        return True
    else:
        return False


def translate_column(df, column):
    if st.session_state.translate == "Yes":
        return (
            df.with_column(
                "LANGUAGE",
                F.call_udf("voice_of_the_customer.app.detect", F.col(column)),
            )
            .with_column(
                f"TRANSLATED_{column}",
                F.when(F.col("LANGUAGE") == F.lit("en"), F.col(column)).otherwise(
                    F.call_builtin(
                        "SNOWFLAKE.CORTEX.TRANSLATE",
                        F.col(column),
                        F.col("LANGUAGE"),
                        F.lit("en"),
                    )
                ),
            )
            .drop(column)
            .with_column_renamed(f"TRANSLATED_{column}", column)
        )
    return df


def summarize_column(df, column):
    return df.with_column(
        "TRANSCRIPT_SUMMARY",
        F.call_builtin(
            "SNOWFLAKE.CORTEX.SUMMARIZE",
            F.col(column),
        ),
    )


def extract_topics(df, column):
    grouping_column = st.session_state.grouping_column
    if st.session_state.grouping_column:
        topics = (
            df.group_by(grouping_column)
            .agg(F.array_agg(F.col(column), is_distinct=True).alias(f"{column}_ARRAY"))
            .with_column(
                "TOP_TOPICS",
                F.call_builtin(
                    "SNOWFLAKE.CORTEX.COMPLETE",
                    F.lit("mistral-large2"),
                    F.concat(
                        F.lit(
                            "[SYSTEM] You are assigned to review various conversations and text snippets. Your role is to describe the issue. For example, Gloves missing price tags. Helmets were delivered with scratches."
                        ),
                        F.col(f"{column}_ARRAY").cast(T.StringType()),
                        F.lit(
                            "[USER] What are the main topics discussed by the customers? Do not respond with any preamble. Do not include 'Based on the conversations'. Do not include the name of the product in the topic."
                        ),
                    ),
                ),
            )
            .drop(f"{column}_ARRAY")
            .with_column("TOPICS_SPLIT", F.split(F.col("TOP_TOPICS"), F.lit("\n")))
            .flatten(F.col("TOPICS_SPLIT"))
            .select(
                grouping_column,
                F.regexp_replace(F.trim(F.col("VALUE")), r"^[-\d.]+\s*", "").alias(
                    "TOPIC"
                ),
            )
            .group_by(grouping_column)
            .agg(F.array_agg(F.col("TOPIC")).alias("TOPIC_ARRAY"))
        )
    else:
        topics = (
            df.with_column(
                f"{column}_PARSED",
                F.parse_json(
                    F.call_builtin(
                        "SNOWFLAKE.CORTEX.COMPLETE",
                        "mistral-large2",
                        F.concat(
                            F.lit(
                                "Extract 5 topics from this field in an array format."
                            ),
                            F.col(column),
                            F.lit(
                                "Do not include any preambles. Do not include any backticks."
                            ),
                        ),
                    ),
                ),
            )
            .flatten(f"{column}_PARSED")
            .select(F.col("VALUE").cast(T.StringType()).alias("VALUE"))
            .agg(F.array_agg(F.col("VALUE"), is_distinct=True).alias("TOPICS"))
            .with_column(
                "TOPIC_ARRAY",
                F.parse_json(
                    F.call_builtin(
                        "SNOWFLAKE.CORTEX.COMPLETE",
                        F.lit("mistral-large2"),
                        F.concat(
                            F.lit("Given this list of topics: "),
                            F.col("TOPICS").cast(T.StringType()),
                            F.lit(
                                "- Combine any that are similar only returning the primary topic. Do not include any preamble. This should be a list of only ten. This should be in an array format."
                            ),
                        ),
                    ),
                ),
            )
        ).drop("TOPICS")
    return topics


def extract_sentiment(df, column):
    return df.with_column(
        "SENTIMENT",
        F.call_builtin("SNOWFLAKE.CORTEX.SENTIMENT", F.col(column)),
    )


def classify_topics(df, topics, column):
    if st.session_state.grouping_column:
        df = df.join(topics, [st.session_state.grouping_column])
        df = df.with_column(
            "TOPIC",
            F.call_builtin(
                "SNOWFLAKE.CORTEX.CLASSIFY_TEXT",
                F.col(column),
                F.col("TOPIC_ARRAY"),
            )["label"].cast(T.StringType()),
        )
    else:
        df = df.cross_join(topics)
        df = df.with_column(
            "TOPIC",
            F.call_builtin(
                "SNOWFLAKE.CORTEX.CLASSIFY_TEXT",
                F.col(column),
                F.col("TOPIC_ARRAY"),
            )["label"].cast(T.StringType()),
        )
    return df.drop("TOPIC_ARRAY")


def run_selections():
    progress_bar = st.progress(0)
    clear_results_table()
    if st.session_state.translate == "Yes":
        progress_bar.progress(0.1, "Translating")
        df = translate_column(
            st.session_state.df, st.session_state.selected_column
        ).cache_result()

    progress_bar.progress(0.3, "Extracting Topics")
    topics = extract_topics(df, st.session_state.selected_column).cache_result()
    progress_bar.progress(0.6, "Classifying Topics")
    df = classify_topics(df, topics, st.session_state.selected_column).cache_result()
    progress_bar.progress(0.8, "Extracting Sentiment")
    df = extract_sentiment(df, st.session_state.selected_column).cache_result()
    progress_bar.progress(0.9, "Summarizing")
    st.session_state.result_df = summarize_column(
        df, st.session_state.selected_column
    ).cache_result()
    progress_bar.progress(1.0, "Complete")
    progress_bar.empty()

    save_run()


def clear_results_table():
    if "result_df" in st.session_state:
        del st.session_state.result_df


def save_run():
    session = st.session_state.session  # noqa: F841
    # df = session.create_dataframe(st.session_state.result_df)
    # df.write.mode("overwrite").save_as_table("VOICE_OF_THE_CUSTOMER.APP.my_table")
    # st.write("Saved")


class VOC(BasePage):
    def __init__(self):
        self.name = "voc_page"

    def print_page(self):
        session = st.session_state.session

        st.markdown("**Step 1: Select your Table/View/Stream**")
        with st.container(border=True):
            databases = fetch_databases(session)
            st.session_state.selected_database = st.selectbox("Databases:", databases)

            # Based on selected database, fetch schemas and populate the dropdown
            schemas = fetch_schemas(session, st.session_state.selected_database)
            st.session_state.selected_schema = st.selectbox(
                f"Schemas in {st.session_state.selected_database}:", schemas
            )

            # Based on selected database and schema, fetch tables and populate the dropdown
            tables = fetch_tables(
                session,
                st.session_state.selected_database,
                st.session_state.selected_schema,
            )
            views = fetch_views(
                session,
                st.session_state.selected_database,
                st.session_state.selected_schema,
            )
            streams = fetch_streams(
                session,
                st.session_state.selected_database,
                st.session_state.selected_schema,
            )
            st.session_state.selected_table = st.selectbox(
                f"Tables/Views/Streams in {st.session_state.selected_database}.{st.session_state.selected_schema}:",
                tables + views + streams,
            )

        if check_fully_qualified():
            st.markdown("**Step 2: Select your column.**")
            st.session_state.df = session.table(
                st.session_state.qualified_selected_table
            )
            st.dataframe(
                st.session_state.df.limit(10),
                key="column",
                on_select="rerun",
                selection_mode="single-column",
                hide_index=True,
            )

            if (
                selected_column := st.session_state["column"]
                .get("selection")
                .get("columns")
            ):
                st.session_state.selected_column = selected_column[0]

                with st.container(border=True):
                    col1_step, col2_step = st.columns((4, 4))
                    with col1_step:
                        st.markdown("**Step 3: Translation (Optional)**")
                        st.segmented_control(
                            "Translation required?", ["Yes", "No"], key="translate"
                        )
                    with col2_step:
                        st.markdown("**Step 4: Grouping (Optional)**")

                        if len(st.session_state.df.columns) < 13:
                            st.pills(
                                "Select a grouping column:",
                                [
                                    col
                                    for col in st.session_state.df.columns
                                    if col != st.session_state.selected_column
                                ],
                                key="grouping_column",
                            )
                        else:
                            st.multiselect(
                                "Select a grouping column:",
                                [
                                    col
                                    for col in st.session_state.df.columns
                                    if col != st.session_state.selected_column
                                ],
                                key="grouping_column",
                            )
                _, _, col3_run = st.columns((6, 6, 2))
                with col3_run:
                    st.button(
                        label="Run",
                        key="run",
                        help="Run selections",
                        on_click=run_selections,
                        use_container_width=True,
                    )

                if (
                    "result_df" in st.session_state
                    and st.session_state.result_df is not None
                ):
                    st.dataframe(
                        st.session_state.result_df,
                        hide_index=True,
                        use_container_width=True,
                    )

    def print_sidebar(self):
        super().print_sidebar()
