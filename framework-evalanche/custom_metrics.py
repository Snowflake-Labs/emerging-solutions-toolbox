# Python 3.8 type hints
from typing import Union

from src.metrics import Metric
from src.prompts import *
from src.snowflake_utils import run_async_sql_complete

"""
The below commented code is an example of a custom metric that can be added to the Evalanche.
Every metric must have an __init__ method that initializes the metric with a name, description, and required inputs.
Most metrics will be LLM-as-a-judge, in which case the __init__ should also have a prompt argument with f-strings that correspond to the required inputs.

The get_prompt method is used to format the prompt with the required inputs.
The evaluate method is used to run the prompt and return the metric value.

Lastly, the custom metric must be added to the custom_metrics list at the bottom of this file.

After updating this file, you will need to re-compress the src directory and re-upload to stage. Otherwise, the app will not be able to use the new custom metric(s).
"""

# Custom Metrics
# class CustomRelevancy(Metric):
#     def __init__(
#         self,
#     ):
#         super().__init__(
#             name="CustomRelevancy",
#             description="Evaluates the correctness, relevance, and helpfulness of a response compared to a reference answer.",
#             prompt=Relevance_prompt,
#             required={
#                 "question": "User question",
#                 "answer_ref": "Expected answer to the question",
#                 "ai_response": "LLM-generated response to the question",
#             },
#         )

#     def get_prompt(
#         self, question: str, answer_ref: str, ai_response: str
#     ) -> Union[str, None]:
#         if self.prompt is not None:
#             fstrings = {
#                 "question": question,
#                 "answer_ref": answer_ref,
#                 "ai_response": ai_response,
#             }
#             return self.prompt.format(**fstrings)
#         else:
#             return None

#     def evaluate(
#         self,
#         question: str,
#         answer_ref: str,
#         ai_response: str,
#         model: str = "llama3.1-8b",
#     ):
#         import re

#         prompt = self.get_prompt(question, answer_ref, ai_response)

#         response = run_async_sql_complete(self.session, model, prompt)
#         values = [str(i) for i in range(1, 11)]
#         pattern = f"[{''.join(values)}]"
#         match = re.search(pattern, response)

#         return int(match.group(0)) if match else None

custom_metrics = [
    # CustomRelevancy()
]
