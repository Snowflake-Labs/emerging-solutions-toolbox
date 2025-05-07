import streamlit as st
from appPages.page import BasePage, set_page, col, Image, base64, pd, time


def add_collection_or_entity(is_existing_collection):
    if is_existing_collection:
        st.session_state.target_admin_existing = True
    else:
        st.session_state.target_admin_existing = False


def add_existing():
    st.session_state.existing = True


def fresh_install_button_click():
    st.session_state.target_admin_existing = False
    set_page("target_creation")

@st.dialog("Add")
def add_new_object():
    if "existing" not in st.session_state:
        st.session_state.existing = False

    if st.session_state.existing:
        st.session_state.target_selection = st.selectbox(
            "Please select the target collection?",
            st.session_state.target_names_pd['TARGET_COLLECTION_NAME'], key='1')
        if st.button("Confirm",
                     use_container_width=True,
                     on_click=add_collection_or_entity,
                     args=(True,), ):
            set_page("target_creation")
            st.rerun()

    if not st.session_state.existing:
        st.write("Add to existing Collection?")
        col1_existing, col2_existing = st.columns(
            (.25, .25)
        )
        if len(st.session_state.collection_entity_list_pd) > 0:
            with col1_existing:
                st.button("Yes",
                          use_container_width=True,
                          on_click=add_existing, )

        with col2_existing:
            if st.button("No",
                         use_container_width=True,
                         on_click=add_collection_or_entity,
                         args=(False,), ):
                set_page("target_creation")
                st.rerun()


@st.dialog("Confirm Deletion?")
def confirm_delete_entity(target_collection_name, collection_entity_name):
    st.session_state.selected_target_collection = target_collection_name
    st.session_state.collection_entity_name = collection_entity_name

    st.write(f"Are you sure you want to delete {collection_entity_name}?")

    col1_del, col2_del = st.columns(
        (.25, .25)
    )
    with col1_del:
        if st.button("Yes",
                     use_container_width=True,
                     on_click=delete_entity
                     ):
            st.success("Deleted Successfully")
            time.sleep(2)
            st.rerun()

    with col2_del:
        if st.button("No",
                     use_container_width=True):
            st.rerun()


def delete_entity():
    session = st.session_state.session
    count_target_collections_sql = "SELECT COUNT(TARGET_ENTITY_NAME) AS DOES_EXIST FROM " + st.session_state.native_database_name + ".admin.TARGET_ENTITY WHERE TARGET_COLLECTION_NAME = '" + st.session_state.selected_target_collection + "'"
    collection_run = session.sql(count_target_collections_sql).collect()

    collection_run_pd = pd.DataFrame(collection_run)

    target_collection_del_sql = "DELETE FROM " + st.session_state.native_database_name + ".admin.TARGET_COLLECTION WHERE TARGET_COLLECTION_NAME = '" + st.session_state.selected_target_collection + "'"
    # If there are no more entities tied to collection go ahead and delete target collection
    if int(collection_run_pd.loc[0, "DOES_EXIST"]) == 1:
        try:
            collection_run = session.sql(target_collection_del_sql).collect()
        except Exception as e:
            st.info(e)

    target_entity_del_sql = "DELETE FROM " + st.session_state.native_database_name + ".admin.TARGET_ENTITY WHERE TARGET_COLLECTION_NAME = '" + st.session_state.selected_target_collection + "' AND TARGET_ENTITY_NAME= '" + st.session_state.collection_entity_name + "'"
    target_entity_attribute_del_sql = "DELETE FROM " + st.session_state.native_database_name + ".admin.TARGET_ENTITY_ATTRIBUTE WHERE TARGET_COLLECTION_NAME = '" + st.session_state.selected_target_collection + "' AND TARGET_ENTITY_NAME= '" + st.session_state.collection_entity_name + "'"

    # Delete from target entity table
    try:
        collection_run = session.sql(target_entity_del_sql).collect()
    except Exception as e:
        st.info(e)

    # Delete from target entity attribute table
    try:
        collection_run = session.sql(target_entity_attribute_del_sql).collect()
    except Exception as e:
        st.info(e)

    count_target_collections_sql = "SELECT COUNT(*) AS DOES_EXIST FROM " + st.session_state.native_database_name + ".admin.TARGET_ENTITY WHERE TARGET_COLLECTION_NAME = '" + st.session_state.selected_target_collection + "'"
    collection_run = session.sql(count_target_collections_sql).collect()

    collection_run_pd = pd.DataFrame(collection_run)




class TargetAdministration(BasePage):
    def __init__(self):
        self.name = "target_admin"

    def print_page(self):
        session = st.session_state.session
        # Clear out selections if previously configuring other
        if 'existing' in st.session_state:
            del st.session_state.existing
        if 'target_selection' in st.session_state:
            del st.session_state.target_selection
        if 'entity_table_display' in st.session_state:
            del st.session_state.entity_table_display
        if 'target_collection_name' in st.session_state:
            del st.session_state.target_collection_name
        if 'table_preview_df' in st.session_state:
            del st.session_state.table_preview_df

        st.session_state.target_names_pd = (
            session.table(st.session_state.native_database_name + ".ADMIN.TARGET_COLLECTION")
            .select(col("TARGET_COLLECTION_NAME"))
            .distinct()
            .to_pandas()
        )

        st.session_state.collection_entity_list_pd = (
            session.table(st.session_state.native_database_name + ".ADMIN.TARGET_ENTITY")
            .sort(col("TARGET_COLLECTION_NAME").asc())
            .select(col("TARGET_COLLECTION_NAME"), col("TARGET_ENTITY_NAME"))
            .distinct()
            .to_pandas()
        )
        col1_header, col2_header, col3_header, col4_header, col5_header = st.columns(
            (5, 1, 1, 1, 1)
        )
        with col1_header:
            st.header("Target Collections")

        if len(st.session_state.collection_entity_list_pd) > 0:
            with col5_header:
                st.button(
                    "Add New",
                    key="add_new",
                    use_container_width=True,
                    on_click=add_new_object,
                    type="primary",
                    # args=(True,),
                )
        else:
            with col5_header:
                st.button(
                    "Add New",
                    key="add_new_fresh",
                    use_container_width=True,
                    on_click=fresh_install_button_click,
                    type="primary",
                    # args=(True,),
                )

        if len(st.session_state.collection_entity_list_pd) > 0:
            for i in range(len(st.session_state.collection_entity_list_pd)):
                if i == 0:
                    st.subheader(st.session_state.collection_entity_list_pd.loc[i, "TARGET_COLLECTION_NAME"])

                else:
                    if (st.session_state.collection_entity_list_pd.loc[i - 1, "TARGET_COLLECTION_NAME"] !=
                            st.session_state.collection_entity_list_pd.loc[i, "TARGET_COLLECTION_NAME"]):
                        st.subheader(st.session_state.collection_entity_list_pd.loc[i, "TARGET_COLLECTION_NAME"])

                target_collection_name = st.session_state.collection_entity_list_pd.loc[i, "TARGET_COLLECTION_NAME"]

                with st.container(border=True):
                    collection_entity_name = st.session_state.collection_entity_list_pd.loc[i, "TARGET_ENTITY_NAME"]

                    col1_ex, col2_ex, col3_ex, col4_ex, col5_ex = st.columns(
                        (0.1, 1, 2, 1, 1)
                    )

                    with col1_ex:
                        col1_ex.empty()

                    with col2_ex:
                        if st.session_state.streamlit_mode != "OSS":
                            if i == 0 or i == 1:
                                image_name = "Images/collection.png"
                            else:
                                image_name = "Images/collection2.png"
                            mime_type = image_name.split(".")[-1:][0].lower()
                            with open(image_name, "rb") as f:
                                content_bytes = f.read()
                                content_b64encoded = base64.b64encode(
                                    content_bytes
                                ).decode()
                                image_name_string = (
                                    f"data:image/{mime_type};base64,{content_b64encoded}"
                                )
                                st.image(image_name_string, width=90)
                        else:
                            if i == 0 or i == 1:
                                dataimage = Image.open("toStage/streamlit/Images/collection.png")
                            else:
                                dataimage = Image.open("toStage/streamlit/Images/collection2.png")

                            st.image(dataimage, width=90)

                    with col3_ex:
                        st.subheader(collection_entity_name)

                    with col5_ex:
                        st.button(
                            "Delete",
                            key=str(i) + "Delete",
                            use_container_width=True,
                            on_click=confirm_delete_entity,
                            type="primary",
                            args=(target_collection_name, collection_entity_name),
                        )
        else:
            st.write("Welcome! To start your Target Collection Building, please select the Add New Button")

    def print_sidebar(self):
        super().print_sidebar()
