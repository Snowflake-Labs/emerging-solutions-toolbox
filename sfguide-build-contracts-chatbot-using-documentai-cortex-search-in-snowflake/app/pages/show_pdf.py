import streamlit as st

if "paginator" in st.session_state:
    paginator = st.session_state["paginator"]

    st.info(f"You are currently viewing the file at {paginator.stage_location}.")

    st.image(paginator.render_page())

    col1, col2, col3 = st.columns(spec=[0.55, 2, 1], gap="large")

    col1.button(
        "⬅️",
        on_click=lambda: paginator.change_page("previous"),
        disabled=True if paginator.current_page == 0 else False,
    )
    col2.slider(
        "Page Slider",
        min_value=1,
        max_value=paginator.total_pages,
        value=paginator.current_page + 1,
        on_change=lambda: paginator.change_page("slider"),
        key="slider_key",
        label_visibility="collapsed",
    )
    col3.button(
        "➡️",
        on_click=lambda: paginator.change_page("next"),
        disabled=True if paginator.current_page == paginator.total_pages - 1 else False,
    )
