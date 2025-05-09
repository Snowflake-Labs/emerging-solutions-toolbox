from appPages.page import (BasePage, pd, st, time,
                           col, when_matched, when_not_matched, current_timestamp, parse_json, set_page)


def insert_attribute_selections():
    session = st.session_state.session
    attribute_edited_pd = pd.DataFrame(st.session_state.attribute_edited)

    attr_data = []

    for index, row in attribute_edited_pd.iterrows():
        attr_data.append(
            {
                "TARGET_COLLECTION_NAME": st.session_state.collection_name,
                "TARGET_ENTITY_NAME": st.session_state.entity_name,
                "TARGET_ENTITY_ATTRIBUTE_NAME": row['TARGET ENTITY ATTRIBUTE NAME'],
                "INCLUDE_IN_ENTITY": row['INCLUDE IN ENTITY']
            })
    try:
        initial_data_df = session.create_dataframe(attr_data)
    except Exception as e:
        st.info(e)

    target_df = session.table(st.session_state.native_database_name + ".admin.TARGET_ENTITY_ATTRIBUTE")

    try:
        target_df.merge(
            initial_data_df,
            (target_df["TARGET_COLLECTION_NAME"] == initial_data_df["TARGET_COLLECTION_NAME"]) &
            (target_df["TARGET_ENTITY_ATTRIBUTE_NAME"] == initial_data_df["TARGET_ENTITY_ATTRIBUTE_NAME"]) &
            (target_df["TARGET_ENTITY_NAME"] == initial_data_df["TARGET_ENTITY_NAME"]),
            [
                when_matched().update(
                    {
                        "INCLUDE_IN_ENTITY": initial_data_df["INCLUDE_IN_ENTITY"],
                        "LAST_UPDATED_TIMESTAMP": current_timestamp(),
                    }
                ),
            ],
        )
    except Exception as e:
        st.info(e)

    with st.spinner('Successfully Saved Target Entity Attributes! Redirecting in 5 seconds...'):
        time.sleep(5)
        set_page("target_admin")

def update_preview():
    st.session_state.edited_preview = st.session_state.entity_table_df.filter(col("INCLUDE_IN_ENTITY") == 'TRUE')


class TargetEntitySelection(BasePage):
    def __init__(self):
        self.name = "target_entity_selection"

    def print_page(self):
        session = st.session_state.session
        css = """
        <style>
            .st-key-save_button{
                #padding-bottom: 4em;
                display: block !important;
                #text-align: right !important;
                bottom: 1em !important;
                right: 10px !important;

            }
        </style>"""
        st.html(css)
        st.session_state.entity_table_df = (
            session.table(st.session_state.native_database_name + ".admin.TARGET_ENTITY_ATTRIBUTE")
            .filter(col("TARGET_COLLECTION_NAME") == st.session_state.collection_name)
            .filter(col("TARGET_ENTITY_NAME") == st.session_state.entity_name)
        )

        entity_table_filtered = st.session_state.entity_table_df.select(
            st.session_state.entity_table_df.TARGET_ENTITY_ATTRIBUTE_NAME,
            st.session_state.entity_table_df.TARGET_ATTRIBUTE_PROPERTIES[
                "data_type"
            ],
            st.session_state.entity_table_df.INCLUDE_IN_ENTITY
        ).to_pandas()

        entity_table_filtered.columns = ["TARGET ENTITY ATTRIBUTE NAME", "DATA TYPE",
                                         "INCLUDE IN ENTITY"]

        # Remove Quotes in column
        entity_table_filtered['DATA TYPE'] = entity_table_filtered['DATA TYPE'].str.replace(r'"', '')

        st.session_state.entity_table_display = entity_table_filtered

        with st.container(border=True):
            st.markdown("**Selected Entity Preview:** " + st.session_state.qualified_selected_table)
            if 'table_preview_df' not in st.session_state:
                st.session_state.table_preview_df = (
                    session.table(st.session_state.qualified_selected_table)
                    .limit(10)
                     )
                st.data_editor(st.session_state.table_preview_df, hide_index=True, disabled=True, use_container_width=True)
            else:
                st.data_editor(st.session_state.table_preview_df, hide_index=True, disabled=True,use_container_width=True)

        col1_entity, col2_entity, col3_entity, col4_entity = st.columns(
            (4, 1, .25, 0.5)
        )

        with col1_entity:
            st.markdown('**Choose which attributes to include in your Selected Entity:**')
            st.session_state.attribute_edited = st.data_editor(st.session_state.entity_table_display,
                                                               disabled=["TARGET ENTITY ATTRIBUTE NAME", "DATA TYPE"],
                                                               hide_index=True, on_change=update_preview,use_container_width=True)
        st.write('')
        with col4_entity:
            st.title('')
            st.title('')
            st.title('')
            st.write('')
            st.write('')
            st.write('')
            done_adding_button = st.button(
                "Save",
                key="save_button",
                on_click=insert_attribute_selections,
                type="primary",

            )

    def print_sidebar(self):
        super().print_sidebar()
