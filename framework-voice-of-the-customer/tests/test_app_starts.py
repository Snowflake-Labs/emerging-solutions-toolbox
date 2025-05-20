from streamlit.testing.v1 import AppTest


def test_app():
    at = AppTest.from_file(
        "app_main.py",
        default_timeout=30,
    )
    at.run()
    assert not at.exception
