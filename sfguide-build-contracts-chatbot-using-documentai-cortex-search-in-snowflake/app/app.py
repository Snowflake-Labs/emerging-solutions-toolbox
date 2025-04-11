from io import BytesIO

import pandas as pd
import pypdfium2 as pdfium
import streamlit as st
from snowflake.core import Root
from snowflake.cortex import Complete
from snowflake.snowpark.context import get_active_session

MODELS = [
    "mistral-large",
    "snowflake-arctic",
    "llama3-70b",
    "llama3-8b",
]


def initialize_app():
    """
    Initializes the Streamlit application by setting up session state variables,
    configuring the sidebar with various options, and preparing the search service
    metadata.

    This function performs the following tasks:
    1. Clears the conversation if required or initializes the messages list.
    2. Retrieves and stores metadata for available Cortex search services.
    3. Configures the sidebar with options to select a search service, clear the
       conversation, toggle debug mode, and use chat history.
    4. Provides advanced options for selecting a model, number of context chunks,
       and number of messages to use in chat history.
    5. Displays the current session state in the sidebar.
    6. Retrieves and stores attribute columns for the selected search service.
    7. Allows the user to select filters based on the attribute columns.

    Note:
        This function relies on the `st` (Streamlit) and `session` objects being
        available in the global scope.

    Raises:
        KeyError: If required keys are missing from the session state.
    """
    if st.session_state.get("clear_conversation") or "messages" not in st.session_state:
        st.session_state.messages = []

    if "service_metadata" not in st.session_state:
        services = session.sql("SHOW CORTEX SEARCH SERVICES IN DATABASE;").collect()
        st.session_state.service_metadata = [
            {
                "name": f"{s['schema_name']}.{s['name']}",
                "search_column": session.sql(
                    f"DESC CORTEX SEARCH SERVICE {s['schema_name']}.{s['name']};"
                ).collect()[0]["search_column"],
            }
            for s in services
        ]

    search_services = [s["name"] for s in st.session_state.service_metadata]

    st.sidebar.selectbox(
        "Select cortex search service:",
        search_services,
        key="selected_cortex_search_service",
        index=search_services.index(
            st.session_state.get("chosen_service", search_services[0])
        ),
        on_change=lambda: st.session_state.update(
            {"chosen_service": st.session_state.selected_cortex_search_service}
        ),
    )

    st.sidebar.button("Clear conversation", key="clear_conversation")
    st.sidebar.button(
        "Remove selected document",
        key="clear_document",
        on_click=remove_document_filter(),
    )
    st.sidebar.toggle("Debug", key="debug", value=False)
    st.sidebar.toggle("Use chat history", key="use_chat_history", value=True)

    with st.sidebar.expander("Advanced options"):
        st.selectbox("Select model:", MODELS, key="model_name")
        st.number_input(
            "Select number of context chunks",
            value=5,
            key="num_retrieved_chunks",
            min_value=1,
            max_value=10,
        )
        st.number_input(
            "Select number of messages to use in chat history",
            value=5,
            key="num_chat_messages",
            min_value=1,
            max_value=10,
        )

    st.sidebar.expander("Session State").write(st.session_state)

    search_service_result = session.sql(
        f"""DESC CORTEX SEARCH SERVICE {st.session_state.selected_cortex_search_service}"""
    ).collect()[0]
    attribute_columns = search_service_result.attribute_columns
    st.session_state.attribute_columns = (
        attribute_columns.split(",") if attribute_columns else []
    )

    if st.session_state.attribute_columns:
        st.multiselect(
            ":mag_right: Select the set of filters you want to use",
            [c for c in st.session_state.attribute_columns if c != "STAGE_PATH"],
            default=None,
            placeholder="Please Select filter",
            key="attributes",
        )
        st.session_state.attribute_dict = dict.fromkeys(
            st.session_state.attributes, "Yes"
        )


def query_cortex_search_service(query, columns=[], filter={}):
    """
    Queries the Cortex Search Service with the given parameters and retrieves context documents.

    Args:
        query (str): The search query string.
        columns (list, optional): List of columns to include in the search. Defaults to an empty list.
        filter (dict, optional): Dictionary of filters to apply to the search. Defaults to an empty dictionary.

    Returns:
        tuple: A tuple containing:
            - context_str (str): A formatted string of the context documents.
            - results (list): A list of search results from the Cortex Search Service.
    """
    db, schema, service_name = (
        session.get_current_database(),
        *st.session_state.selected_cortex_search_service.split("."),
    )

    cortex_search_service = (
        root.databases[db].schemas[schema].cortex_search_services[service_name]
    )

    context_documents = cortex_search_service.search(
        query,
        columns=columns,
        filter=filter,
        limit=st.session_state.num_retrieved_chunks,
    )
    results = context_documents.results

    search_col = next(
        s["search_column"]
        for s in st.session_state.service_metadata
        if s["name"] == st.session_state.selected_cortex_search_service
    ).lower()

    context_str = "\n".join(
        f"Context document {i+1}: {r[search_col]} \n" for i, r in enumerate(results)
    )

    if st.session_state.debug:
        st.sidebar.text_area("Context documents", context_str, height=500)

    return context_str, results


def get_chat_history():
    """
    Retrieve the most recent chat messages from the session state.

    Returns:
        list: A list of the most recent chat messages, limited by the number
              specified in the session state's 'num_chat_messages' attribute.
    """
    return st.session_state.messages[-st.session_state.num_chat_messages :]


def make_chat_history_summary(chat_history, question):
    """
    Generates a natural language query that extends the given question using the provided chat history.

    Args:
        chat_history (str): The chat history to be used for generating the query.
        question (str): The question to be extended with the chat history.

    Returns:
        str: A natural language query that incorporates the chat history and the question.
    """
    prompt = f"""
        [INST]
        Based on the chat history below and the question, generate a query that extend the question
        with the chat history provided. The query should be in natural language.
        Answer with only the query. Do not add any explanation.

        <chat_history>
        {chat_history}
        </chat_history>
        <question>
        {question}
        </question>
        [/INST]
    """

    summary = Complete(st.session_state.model_name, prompt).replace("$", r"\$")

    if st.session_state.debug:
        st.sidebar.text_area(
            "Chat history summary", summary.replace("$", r"\$"), height=150
        )

    return summary


def remove_document_filter():
    if st.session_state.get("clear_document") and "STAGE_PATH" in st.session_state.get(
        "filter_dict", {}
    ):
        st.session_state.get("filter_dict", {}).pop("STAGE_PATH")


def add_document_filter():
    st.session_state.filter_dict = st.session_state.attribute_dict.copy()
    if st.session_state.get("filter_yes"):
        st.session_state.filter_dict |= {
            "STAGE_PATH": st.session_state["paginator"].stage_location
        }


def create_prompt(user_question):
    """
    Generates a prompt for an AI chat assistant with Retrieval-Augmented Generation (RAG) capabilities.

    This function constructs a prompt that includes the user's question, chat history, and context derived
    from a search service. The prompt is formatted to guide the AI assistant in providing a coherent and
    concise answer relevant to the user's question.

    Args:
        user_question (str): The question asked by the user.

    Returns:
        tuple: A tuple containing the formatted prompt (str) and the search results (any).
    """
    search_params = dict(
        columns=["chunk", "stage_path", "relative_path"],
        filter=(
            {
                "@and": [
                    {"@or": [{"@eq": {column: column_values}}]}
                    for column, column_values in st.session_state.get(
                        "filter_dict", {}
                    ).items()
                    if column_values
                ]
            }
            if st.session_state.get("filter_dict", {})
            else {}
        ),
    )

    if st.session_state.use_chat_history:
        chat_history = get_chat_history()
        if chat_history:
            question_summary = make_chat_history_summary(chat_history, user_question)
            prompt_context, results = query_cortex_search_service(
                question_summary,
                **search_params,
            )
        else:
            prompt_context, results = query_cortex_search_service(
                user_question,
                **search_params,
            )
    else:
        prompt_context, results = query_cortex_search_service(
            user_question,
            **search_params,
        )
        chat_history = ""

    prompt = f"""
            [INST]
            You are a helpful AI chat assistant with RAG capabilities. When a user asks you a question,
            you will also be given context provided between <context> and </context> tags. Use that context
            with the user's chat history provided in the between <chat_history> and </chat_history> tags
            to provide a summary that addresses the user's question. Ensure the answer is coherent, concise,
            and directly relevant to the user's question.

            If the user asks a generic question which cannot be answered with the given context or chat_history,
            just say "I don't know the answer to that question.

            Don't say things like "according to the provided context".

            <chat_history>
            {chat_history}
            </chat_history>
            <context>
            {prompt_context}
            </context>
            <question>
            {user_question}
            </question>
            [/INST]
            Answer:
            """
    return prompt, results


class PdfPaginator:
    """
    PdfPaginator is a class for handling PDF pagination and rendering.

    Methods:
        __init__(stage_location: str):
            Initializes the PdfPaginator with the given stage location.

        download_data(stage_location: str):
            Downloads the PDF data from the given stage location.

        change_page(direction: str):
            Changes the current page in the PDF document based on the direction.

        render_page():
            Renders the current page of the PDF document and returns it as a PIL image.
    """

    def __init__(self, stage_location: str):
        """
        Initializes the application with the given stage location.

        Args:
            stage_location (str): The location of the stage data.

        Attributes:
            pdf (pdfium.PdfDocument): The PDF document loaded from the stage location.
            stage_location (str): The location of the stage data.
            current_page (int): The current page number in the PDF document.
            total_pages (int): The total number of pages in the PDF document.
        """
        self.pdf = pdfium.PdfDocument(self.download_data(stage_location))
        self.stage_location = stage_location
        self.current_page = 0
        self.total_pages = len(self.pdf)

    @staticmethod
    @st.cache_data
    def download_data(stage_location: str):
        """
        Downloads data from the specified stage location.

        Args:
            stage_location (str): The location of the data to be downloaded.

        Returns:
            BytesIO: A BytesIO object containing the downloaded data.
        """
        data = BytesIO(session.file.get_stream(stage_location).read())
        return data

    def change_page(self, direction):
        """
        Changes the current page based on the given direction.

        Args:
            direction (str): The direction to change the page.
                             It can be either "next" to go to the next page
                             or "previous" to go to the previous page.

        Raises:
            IndexError: If the direction is "next" and the current page is the last page,
                        or if the direction is "previous" and the current page is the first page.
        """
        if direction == "next" and self.current_page < self.total_pages - 1:
            self.current_page += 1
        elif direction == "previous" and self.current_page > 0:
            self.current_page -= 1
        elif direction == "slider":
            self.current_page = st.session_state.get("slider_key") - 1
        else:
            raise IndexError("Page is out of range.")

    def render_page(self):
        """
        Renders the current page of the PDF as a PIL image.

        Returns:
            PIL.Image.Image: The rendered image of the current page.
        """
        return self.pdf[self.current_page].render(scale=1, rotation=0).to_pil()


def main():
    """
    Main function to run the chatbot application with Snowflake Cortex integration.

    This function initializes the application, sets up the chat interface, handles user input,
    and displays responses from the assistant. It also manages the selection of PDF documents
    and displays relevant dataframes.

    The function performs the following steps:
    1. Sets the title of the Streamlit app.
    2. Initializes the application.
    3. Defines icons for the assistant and user.
    4. Retrieves selected rows from the session state.
    5. Updates the paginator based on the selected rows.
    6. Iterates through the messages in the session state and displays them in the chat interface.
    7. Checks if the chat input should be disabled based on the presence of service metadata.
    8. Handles user input from the chat input box.
    9. Generates a response from the assistant and updates the session state with the new message.
    10. Displays the generated response and relevant dataframe in the chat interface.

    Note:
        This function relies on several external functions and classes such as `initialize_app`,
        `PdfPaginator`, `create_prompt`, and `Complete`, as well as the Streamlit library (`st`).
    """
    st.title(":speech_balloon: Chatbot with Snowflake Cortex")

    initialize_app()

    icons = {"assistant": "❄️", "user": "👤"}

    selected_rows = (
        st.session_state.get("doc_select", default={"selection": {"rows": []}})
        .get("selection")
        .get("rows")
    )

    if selected_rows:
        st.session_state["paginator"] = PdfPaginator(
            st.session_state.df.iloc[selected_rows[0]]["stage_path"]
        )

        # st.write("We're going to ask you to say yes or no here.")
    else:
        rerun_selected_rows = (
            st.session_state.get(
                "rerun_doc_select", default={"selection": {"rows": []}}
            )
            .get("selection")
            .get("rows")
        )
        if rerun_selected_rows and len(st.session_state.messages) > 0:
            message = st.session_state.messages[-1]
            _, last_df = message["content"]
            st.session_state["paginator"] = PdfPaginator(
                last_df.iloc[rerun_selected_rows[0]]["stage_path"]
            )

    total_messages = len(st.session_state.messages)
    for index, message in enumerate(st.session_state.messages, start=1):
        with st.chat_message(message["role"], avatar=icons[message["role"]]):
            if message["role"] == "user":
                st.markdown(message["content"])
            elif message["role"] == "assistant":
                response, dataframe = message["content"]
                st.markdown(response)
                if index < total_messages:
                    st.dataframe(dataframe)
                else:
                    st.dataframe(
                        dataframe,
                        hide_index=True,
                        key="rerun_doc_select",
                        on_select="rerun",
                        selection_mode="single-row",
                    )

    if selected_rows or (
        st.session_state.get("rerun_doc_select", default={"selection": {"rows": []}})
        .get("selection")
        .get("rows")
    ):
        if (
            st.session_state.get("filter_no") is None
            and st.session_state.get("filter_yes") is None
            and "STAGE_PATH" not in st.session_state.get("filter_dict", {})
        ):
            with st.chat_message("document", avatar=icons["assistant"]):
                st.markdown(
                    f"""You've selected {st.session_state['paginator'].stage_location}.

Would you like to continue asking questions of this document?
    """
                )
                col1, col2 = st.columns(2)
                col1.button(
                    "Yes",
                    key="filter_yes",
                    on_click=add_document_filter,
                    use_container_width=True,
                )
                col2.button(
                    "No",
                    key="filter_no",
                    on_click=add_document_filter,
                    use_container_width=True,
                )

    disable_chat = (
        "service_metadata" not in st.session_state
        or not st.session_state.service_metadata
    )
    if question := st.chat_input("Ask a question...", disabled=disable_chat):
        st.session_state.messages.append({"role": "user", "content": question})
        with st.chat_message("user", avatar=icons["user"]):
            st.markdown(question.replace("$", r"\$"))

        with st.chat_message("assistant", avatar=icons["assistant"]):
            st.empty()
            question = question.replace("'", "")
            prompt, results = create_prompt(question)
            df = (
                pd.DataFrame(results)
                if results
                else pd.DataFrame([], columns=["stage_path", "chunk"])
            )
            df = df[["stage_path", "chunk"]]
            st.session_state.df = df
            with st.spinner("Thinking..."):
                generated_response = Complete(
                    st.session_state.model_name, prompt
                ).replace("$", r"\$")
                st.markdown(generated_response)
                st.dataframe(
                    df,
                    hide_index=True,
                    key="doc_select",
                    on_select="rerun",
                    selection_mode="single-row",
                )

        st.session_state.messages.append(
            {"role": "assistant", "content": (generated_response, df)}
        )


if __name__ == "__main__":
    session = get_active_session()
    root = Root(session)
    main()
