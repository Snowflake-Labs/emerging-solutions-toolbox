from appPages.page import (BasePage, pd, st, time,
                           col, when_matched, when_not_matched, current_timestamp, parse_json, set_page)


def fetch_databases():
    session = st.session_state.session
    databases = session.sql("SHOW TERSE DATABASES").collect()
    database_name = [d["name"].lower() for d in databases]
    return database_name


def fetch_schemas(database_name):
    session = st.session_state.session
    schemas = session.sql(f"SHOW TERSE SCHEMAS IN {database_name}").collect()
    schema_name = [s["name"].lower() for s in schemas]
    schema_pd = pd.DataFrame(schema_name)
    schema_name = schema_pd[schema_pd[0] != 'information_schema']
    return schema_name


def fetch_tables(database_name, schema_name):
    session = st.session_state.session
    tables = session.sql(f"SHOW TERSE TABLES IN {database_name}.{schema_name}").collect()
    table_name = [t["name"].lower() for t in tables]
    return table_name


def fetch_views(database_name, schema_name):
    session = st.session_state.session
    views = session.sql(f"SHOW TERSE VIEWS IN {database_name}.{schema_name}").collect()
    view_name = [v["name"] for v in views]
    return view_name


def check_entries():
    error_count = 0
    if st.session_state.collection_name == '' and not st.session_state.target_admin_existing:
        st.error("Please input a collection Name")
        error_count = error_count + 1
    if st.session_state.entity_name == '':
        st.error("Please input an Entity Name")
        error_count = error_count + 1

    return error_count


def save_entity():
    session = st.session_state.session

    if (st.session_state.selected_database is not None
            and st.session_state.selected_schema is not None
            and st.session_state.selected_table is not None):
        st.session_state.qualified_selected_table = (
                st.session_state.selected_database
                + "."
                + st.session_state.selected_schema
                + "."
                + st.session_state.selected_table)

    if 'qualified_selected_table' in st.session_state:
        # If selections have been made
        if check_entries() == 0:
            # If target collection name doesn't exist insert into table
            if not st.session_state.target_admin_existing:
                timest = session.create_dataframe([1]).select(current_timestamp()).collect()
                current_time = timest[0]["CURRENT_TIMESTAMP()"]
                insert_target_sql = (
                            "INSERT INTO " + st.session_state.native_database_name + (".ADMIN.TARGET_COLLECTION VALUES"
                                                                                      "('") +
                            st.session_state.collection_name + "','v1','" + str(
                        current_time) + "')")
                # VALUES ('" + target_collection_name + "' , 'v1' , '" + current_timestamp() + "'"

                run_sql = session.sql(insert_target_sql)
                try:
                    run = run_sql.collect()
                except Exception as e:
                    st.info(e)
            else:
                st.session_state.collection_name = st.session_state.target_selection
            try:
                with st.spinner("Generating Attributes"):
                    session.call(st.session_state.native_database_name +".ADMIN.GENERATE_ATTRIBUTES",
                                 st.session_state.qualified_selected_table, st.session_state.collection_name,
                                 st.session_state.entity_name, 'v1')

            except Exception as e:
                st.info(e)

            set_page("target_entity_selection")
    else:
        st.error("Please make sure you have selected a table entity")


@st.dialog("Preview Table", width="large")
def preview():
    if (st.session_state.selected_database is not None
            and st.session_state.selected_schema is not None
            and st.session_state.selected_table is not None):
        st.session_state.qualified_selected_table = (
                st.session_state.selected_database
                + "."
                + st.session_state.selected_schema
                + "."
                + st.session_state.selected_table)

    session = st.session_state.session
    st.html("<span class='big-dialog'></span>")

    st.write(st.session_state.qualified_selected_table)
    st.dataframe(session.table(st.session_state.qualified_selected_table))

    col1_dialog, col2_dialog, col3_dialog, col4_dialog, col5_dialog, col6_dialog = st.columns(
        (3, 3, .25, 0.5, 1, 2)
    )
    with col6_dialog:
        if st.button(
                "Close",
                key="close_button",
                # on_click=save_entity,
                type="primary",
        ):
            st.rerun()


class TargetCreation(BasePage):
    def __init__(self):
        self.name = "target_creation"

    def print_page(self):
        session = st.session_state.session

        css = """
        <style>
            div[data-testid="stDialog"] div[role="dialog"]:has(.big-dialog) {
                width: 80vw;
                height: 65vh;
                }
            .st-key-close_button{
                # display: block !important;
                # text-align: right !important;
                # padding-bottom: 4em;
                display: block !important;
                text-align: right !important;
                bottom: 1em !important;
                right: 10px !important;
                
            }
        </style>"""
        st.html(css)

        if 'set_initial_values' not in st.session_state:
            if st.session_state.target_admin_existing:
                st.session_state.target_collection_name = st.session_state.target_selection
                st.session_state.disable_collection_name = True
            else:
                st.session_state.target_collection_name = 'Please input Collection Name'
                st.session_state.disable_collection_name = False
            st.session_state.initial_values = True

        st.markdown("**Step 1: Select your target table**")
        with st.container(border=True):
            databases = fetch_databases()
            st.session_state.selected_database = st.selectbox(
                "Databases:", databases
            )

            # Based on selected database, fetch schemas and populate the dropdown
            schemas = fetch_schemas(st.session_state.selected_database)
            st.session_state.selected_schema = st.selectbox(
                f"Schemas in {st.session_state.selected_database}:",
                schemas
            )

            # Based on selected database and schema, fetch tables and populate the dropdown
            tables = fetch_tables(
                st.session_state.selected_database, st.session_state.selected_schema
            )
            st.session_state.selected_table = st.selectbox(
                f"Tables in {st.session_state.selected_database}.{st.session_state.selected_schema}:",
                tables
            )

        st.subheader('')

        col1_ex, col2_ex, col3_ex, col4_ex, col5_ex, col6_ex = st.columns(
            (3, 3, .25, 0.5, 1, 2)
        )
        with col1_ex:
            if st.session_state.target_admin_existing:
                st.write('')
                st.write('')
                st.write('')
            else:
                st.write("Step 2: Input Collection Name")

            st.session_state.collection_name = st.text_input(
                "Collection Name",
                key="collection_name_input",
                placeholder=st.session_state.target_collection_name,
                disabled=st.session_state.disable_collection_name
            )

        with col2_ex:
            if st.session_state.target_admin_existing:
                st.write("Step 2: Input Entity Name")
            else:
                st.write("Step 3: Input Entity Name")
            st.session_state.entity_name = st.text_input(
                "Entity Name",
                key="entity_name_input",
                placeholder="Please input Entity Name",
            )
        if (st.session_state.selected_database is not None
                and st.session_state.selected_schema is not None
                and st.session_state.selected_table is not None):
            with col5_ex:
                st.write('')
                st.write('')
                st.write('')
                st.write('')
                preview_button = st.button(
                    "Preview",
                    key="preview",
                    on_click=preview,
                    type="secondary"
                )
        with col6_ex:
            st.write('')
            st.write('')
            st.write('')
            st.write('')
            done_adding_button = st.button(
                "Continue",
                key="done",
                on_click=save_entity,
                type="primary",

            )

    def print_sidebar(self):
        super().print_sidebar()
