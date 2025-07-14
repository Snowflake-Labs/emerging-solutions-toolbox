from snowflake.snowpark import Session

def cortex_analyst_runner(session: Session, prompt: str, semantic_file_path: str) -> str:
    """Runs Cortex Analyst with the provided prompt and semantic file path.

    Args:
        prompt (str): The prompt to send to Cortex Analyst.
        semantic_file_path (str): The fully-qualified path to the semantic model or view file.

    Returns:
        str: The response from Cortex Analyst.
    """
    import _snowflake
    import json

    def send_message(messages, semantic_file_path):
        """Calls the REST API and returns the response."""
        if semantic_file_path.endswith(".yaml") or semantic_file_path.endswith(".yml"):
            request_body = {
                "messages": messages,
                "semantic_model_file": f"@{semantic_file_path}",
            }
        else:
            request_body = {
                "messages": messages,
                "semantic_view": semantic_file_path,
            }
        resp = _snowflake.send_snow_api_request(
                "POST",
                f"/api/v2/cortex/analyst/message",
                {},
                {},
                request_body,
                {},
                30000,
            )
        if resp["status"] < 400:
            response_content = json.loads(resp["content"])
            return response_content
        else:
            raise Exception(
                f"Failed request with status {resp['status']}: {resp}"
            )

    def process_message(session, prompt, semantic_file_path):
        """Processes a message and adds the response to the chat."""
        messages = []
        messages.append(
            {"role": "user", "content": [{"type": "text", "text": prompt}]}
        )
        response = send_message(messages, semantic_file_path)
        for item in response["message"]["content"]:
            if item["type"] == "sql":
                return item.get("statement", None)
        else:
            return None

    return process_message(session, prompt, semantic_file_path)

def delete_metric(session: Session, metric_name: str) -> str:
    """Deletes metric object from Evalanche app.

    Args:
        metric_name (str): The name of the metric to delete.

    Returns:
        str: A message indicating the result of the deletion.
    """

    STAGE = "@GENAI_UTILITIES.EVALUATION.STREAMLIT_STAGE"
    TABLE = "GENAI_UTILITIES.EVALUATION.CUSTOM_METRICS"
    file_path = f"{STAGE}/{metric_name}.pkl"
    query = f"rm {file_path}"

    try:
        session.sql(query).collect()
        metrics_tbl = session.table(TABLE)
        metrics_tbl.delete(metrics_tbl["METRIC_NAME"] == metric_name)
        return f"{file_path} removed."
    except Exception as e:
        return f"An error occurred: {e}"
