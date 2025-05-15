import streamlit as st
from snowflake.snowpark import functions as F
from snowflake.snowpark import Session
from snowflake.snowpark import types as T
from template_style import sit_style
from page import BasePage


TOPIC_PROMPT = (
    "[SYSTEM] You are assigned to review various conversations and text snippets. Be brief and to the point. Do not provide additional reasoning in the response. "
    "[USER] What are the main topics discussed by the customers? Do not respond with any preamble. Your answer should be in an array format. Do not include any special characters or backticks."
)


def change_page(step: str) -> None:
    if step == "segmented_control":
        st.session_state.page_number = st.session_state.get(
            "styled-segcontrols-astabs_page", step
        )
    else:
        st.session_state.page_number = step
        st.session_state["styled-segcontrols-astabs_page"] = step


def prepare_table(session: Session, query: str):
    if (
        st.session_state.get("styled-sbox-create-table-database")
        and st.session_state.get("styled-sbox-create-table-schema")
        and st.session_state.get("styled-sbox-create-table-name")
    ):
        table_name = f"{st.session_state.get('styled-sbox-create-table-database')}.{st.session_state.get('styled-sbox-create-table-schema')}.{st.session_state.get('styled-sbox-create-table-name')}"

        session.sql(query).write.save_as_table(
            table_name,
            mode="overwrite",
        )
        st.success("Table created successfully!")


def set_state():
    if st.session_state.get("styled-sbox-selected_database"):
        st.session_state.database = st.session_state.get(
            "styled-sbox-selected_database"
        )
    if st.session_state.get("styled-sbox-selected_schema"):
        st.session_state.schema = st.session_state.get("styled-sbox-selected_schema")
    if st.session_state.get("styled-sbox-selected_table"):
        st.session_state.table = st.session_state.get("styled-sbox-selected_table")
    if st.session_state.get("styled-pills-selected_grouping_column"):
        st.session_state.grouping_column = st.session_state.get(
            "styled-pills-selected_grouping_column"
        )
    if st.session_state.get("styled-segcontrols_translate"):
        st.session_state.translate = st.session_state.get(
            "styled-segcontrols_translate"
        )
    return None


def fetch_warehouses(_session: Session) -> None:
    if "warehouses" not in st.session_state:
        st.session_state.warehouses = [
            w["name"].lower() for w in _session.sql("SHOW TERSE WAREHOUSES").collect()
        ]
    return None


def fetch_databases(_session: Session) -> None:
    if "databases" not in st.session_state:
        st.session_state.databases = [
            d["name"].lower() for d in _session.sql("SHOW TERSE DATABASES").collect()
        ]
    return None


def fetch_schemas(session: Session) -> list:
    if "table_list" in st.session_state:
        del st.session_state.table_list
    if "styled-sbox-selected_database" in st.session_state:
        database = st.session_state.get("styled-sbox-selected_database")
        st.session_state.schemas = [
            s["name"].lower()
            for s in session.sql(f"SHOW TERSE SCHEMAS IN {database}").collect()
            if s["name"] != "INFORMATION_SCHEMA"
        ]
        return st.session_state.schemas


def fetch_tables(session: Session) -> None:
    database = st.session_state.get("styled-sbox-selected_database")
    schema = st.session_state.get("styled-sbox-selected_schema")
    if database and schema:
        st.session_state.tables = [
            t["name"].lower()
            for t in session.sql(f"SHOW TERSE TABLES IN {database}.{schema}").collect()
        ]
        st.session_state.views = [
            t["name"].lower()
            for t in session.sql(f"SHOW TERSE VIEWS IN {database}.{schema}").collect()
        ]
        st.session_state.streams = [
            t["name"].lower()
            for t in session.sql(f"SHOW TERSE STREAMS IN {database}.{schema}").collect()
        ]
        st.session_state.table_list = (
            st.session_state.tables + st.session_state.views + st.session_state.streams
        )


def check_fully_qualified() -> bool:
    if (
        st.session_state.get("styled-sbox-selected_database") is not None
        and st.session_state.get("styled-sbox-selected_schema") is not None
        and st.session_state.get("styled-sbox-selected_table") is not None
    ):
        set_state()
        st.session_state.qualified_selected_table = (
            st.session_state.get("database")
            + "."
            + st.session_state.get("schema")
            + "."
            + st.session_state.get("table")
        )
        if "df" in st.session_state:
            del st.session_state.df
        if "result_df" in st.session_state:
            del st.session_state.result_df
        if "topic_df" in st.session_state:
            del st.session_state.topic_df
        if "grouping_column" in st.session_state:
            del st.session_state.grouping_column
        return True
    else:
        return False


def translate_column(df, column):
    if st.session_state.get("translate", "No") == "Yes":
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
    grouping_column = st.session_state.get("grouping_column")
    if grouping_column:
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
            .join_table_function("FLATTEN", "TOPICS_SPLIT")
            .select(
                "*",
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
            .join_table_function("FLATTEN", f"{column}_PARSED")
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
    if st.session_state.get("grouping_column"):
        df = df.join(topics, st.session_state.grouping_column)
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


def generate_topics(cache_result: bool = False):
    topics = extract_topics(st.session_state.df, st.session_state.selected_column)
    st.session_state.topic_df = topics


def run_selections(cache_result: bool = False):
    if "df" in st.session_state:
        df = st.session_state.df
        if st.session_state.get("translate", "No") == "Yes":
            df = translate_column(st.session_state.df, st.session_state.selected_column)
        generate_topics(cache_result)
        df = classify_topics(
            df,
            st.session_state.get("topic_df"),
            st.session_state.selected_column,
        )
        df = extract_sentiment(df, st.session_state.selected_column)
        df = summarize_column(df, st.session_state.selected_column)
        st.session_state.result_df = df


def step_one(session):
    st.markdown("**Step 1: Select your Table/View/Stream**")
    st.write(
        "Please select a table with a column containing text you would like to analyze."
    )
    st.selectbox(
        "Databases:",
        st.session_state.databases,
        index=(
            None
            if st.session_state.get("database") is None
            else st.session_state.databases.index(st.session_state.get("database"))
        ),
        on_change=fetch_schemas,
        args=(session,),
        key="styled-sbox-selected_database",
    )
    st.selectbox(
        (
            f"Schemas in {st.session_state.get('database')}:"
            if st.session_state.get("database") is not None
            else "Schema:"
        ),
        (st.session_state.schemas if "schemas" in st.session_state else []),
        index=(
            None
            if st.session_state.get("schema") is None
            or st.session_state.get("schema", "")
            not in st.session_state.get("schemas", [])
            else st.session_state.get("schemas", []).index(
                st.session_state.get("schema")
            )
        ),
        key="styled-sbox-selected_schema",
        on_change=fetch_tables,
        args=(session,),
    )
    st.selectbox(
        (
            f"Tables/Views/Streams in {st.session_state.get('database')}.{st.session_state.get('schema')}:"
            if st.session_state.get("database") is not None
            and st.session_state.get("schema") is not None
            else "Table/View/Stream:"
        ),
        (
            st.session_state.get("table_list", [])
            if "table_list" in st.session_state
            else []
        ),
        index=(
            None
            if st.session_state.get("table") is None
            or st.session_state.get("table", "")
            not in st.session_state.get("table_list", [])
            else st.session_state.get("table_list", []).index(
                st.session_state.get("table")
            )
        ),
        key="styled-sbox-selected_table",
        on_change=check_fully_qualified,
    )
    next_button(
        True if st.session_state.get("qualified_selected_table") else False,
        "Step 2",
    )


def step_two(session):
    st.markdown("**Step 2: Select your column**")
    st.write("Please select the column you would like to analyze.")
    if "qualified_selected_table" in st.session_state:
        st.session_state.df = session.table(st.session_state.qualified_selected_table)
        st.dataframe(
            st.session_state.df.limit(10),
            key="column",
            on_select="rerun",
            selection_mode="single-column",
            hide_index=True,
        )
        if (
            "column" in st.session_state
            and st.session_state["column"].get("selection")
            and st.session_state["column"]["selection"].get("columns")
        ):
            st.session_state.selected_column = st.session_state["column"]["selection"][
                "columns"
            ][0]
        next_button(st.session_state.get("selected_column") is not None, "Step 3")


def step_three(session):
    st.markdown("**Step 3: Additional Configuration (Optional)**")
    st.write("Translate the orginal text to English if another language is detected.")
    st.write(
        "Choose a column to group the topics by. This is optional. If not selected, the topics will generated from the entire table."
    )
    if (
        "qualified_selected_table" in st.session_state
        and "selected_column" in st.session_state
    ):
        col1_step, col2_step = st.columns((4, 4))
        with col1_step:
            st.segmented_control(
                "Translation required?",
                ["Yes", "No"],
                key="styled-segcontrols_translate",
                on_change=set_state,
                default=st.session_state.get("translate", "No"),
            )
        with col2_step:
            st.pills(
                "Select a grouping column:",
                [
                    col
                    for col in st.session_state.df.columns
                    if col != st.session_state.get("selected_column")
                ],
                key="styled-pills-selected_grouping_column",
                selection_mode="multi",
                default=st.session_state.get("grouping_column", []),
                on_change=set_state,
            )
        next_button(st.session_state.get("selected_column") is not None, "Step 4")


def step_four(session):
    st.markdown("**Step 4: Review (Optional)**")
    st.write("Review the generated topics and the output.")
    st.info("The previews may take a moment to complete.")
    if (
        "qualified_selected_table" in st.session_state
        and "selected_column" in st.session_state
    ):
        topic_tab, output_tab = st.tabs(["Topics", "Output"])
        with topic_tab:
            st.button(
                label="Preview Topics",
                key="primary-button-run-topics",
                help="Run selections",
                on_click=generate_topics,
                args=(True,),
                use_container_width=True,
            )
            if st.session_state.get("topic_df"):
                st.dataframe(
                    st.session_state.topic_df,
                    hide_index=True,
                    use_container_width=True,
                )
        with output_tab:
            if "result_df" not in st.session_state:
                st.button(
                    label="Preview Output",
                    key="primary-button-run-output",
                    help="Run selections",
                    on_click=run_selections,
                    args=(True,),
                    use_container_width=True,
                )
            if st.session_state.get("result_df"):
                st.dataframe(
                    st.session_state.result_df,
                    hide_index=True,
                    use_container_width=True,
                )
        next_button(st.session_state.get("selected_column") is not None, "Step 5")


def step_five(session: Session):
    st.markdown("**Step 5: Save the results (Optional)**")
    st.write("Persist the results to a table.")
    run_selections()
    if "result_df" in st.session_state:
        if "grouping_column" in st.session_state:
            query = st.session_state.result_df.join(
                st.session_state.topic_df,
                st.session_state.grouping_column,
            ).queries.get("queries")[0]
        else:
            query = st.session_state.result_df.cross_join(
                st.session_state.topic_df
            ).queries.get("queries")[0]
        with st.expander("View Query", expanded=False):
            st.code(query, language="sql")
        st.selectbox(
            "Choose a database:",
            st.session_state.get("databases"),
            index=(
                None
                if st.session_state.get("database") is None
                else st.session_state.databases.index(st.session_state.get("database"))
            ),
            key="styled-sbox-create-table-database",
        )
        st.selectbox(
            "Choose a schema:",
            (
                st.session_state.schemas
                if "schemas" in st.session_state
                else st.session_state.get("schema")
            ),
            index=(
                None
                if st.session_state.get("schema") is None
                or st.session_state.get("schema", "")
                not in st.session_state.get("schemas", [])
                else st.session_state.get("schemas", []).index(
                    st.session_state.get("schema")
                )
            ),
            key="styled-sbox-create-table-schema",
        )
        st.text_input(
            "Table Name:",
            key="styled-sbox-create-table-name",
        )
        if "styled-sbox-create-table-name" in st.session_state:
            st.button(
                "Create Table",
                on_click=prepare_table,
                args=(
                    session,
                    query,
                ),
            )


def next_button(is_enabled: bool, page_num: str) -> None:
    _, _, column3 = st.columns((1, 2, 1))
    with column3:
        st.button(
            label="Next",
            key=f"primary-button_topage{page_num}",
            on_click=change_page,
            args=(page_num,),
            use_container_width=True,
            disabled=not is_enabled,
        )


class VOC(BasePage):
    def __init__(self):
        self.name = "voc_page"

    def print_page(self):
        session = st.session_state.session
        fetch_databases(session)
        fetch_warehouses(session)
        set_state()
        st.html(sit_style)

        pages = {
            "Step 1": step_one,
            "Step 2": step_two,
            "Step 3": step_three,
            "Step 4": step_four,
            "Step 5": step_five,
        }

        if "page_number" not in st.session_state:
            change_page("Step 1")
        selected_page = st.session_state.get("page_number", "Step 1")

        st.segmented_control(
            "Label",
            ["Step 1", "Step 2", "Step 3", "Step 4", "Step 5"],
            key="styled-segcontrols-astabs_page",
            label_visibility="collapsed",
            on_change=change_page,
            args=("segmented_control",),
        )

        if selected_page in pages:
            pages[selected_page](session)

    def print_sidebar(self):
        super().print_sidebar()
