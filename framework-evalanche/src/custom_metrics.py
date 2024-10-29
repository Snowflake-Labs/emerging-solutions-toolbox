# Python 3.8 type hints
from typing import Union, Optional

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
# class CustomAnswerRelevancy(Metric):
#     def __init__(
#         self,
#         model: str = "llama3.1-8b"
#     ):
#         super().__init__(
#             name="CustomRelevancy",
#             description="""
# Evaluates the relevance of a response to a user question on a scale of 1-5.
# 5 indicates the scorer strongly agrees that the response is relevant and 1 indicates strong disagreement.""",
#             prompt=AnswerRelevancy_prompt,
#             required={
#                 "question": "User question",
#                 "ai_response": "LLM-generated response to the question",
#             },
#         )
#         self.model = model

#     def get_prompt(
#         self, question: str, ai_response: str
#     ) -> Union[str, None]:
#         if self.prompt is not None:
#             fstrings = {
#                 "question": question,
#                 "ai_response": ai_response,
#             }
#             return self.prompt.format(**fstrings)
#         else:
#             return None

#     def evaluate(
#         self,
#         question: str,
#         ai_response: str,
#         model: Optional[str] = None,
#     ):
#         import re

#         model_to_use = model if model else self.model  

#         prompt = self.get_prompt(question, ai_response)

#         response = run_async_sql_complete(self.session, model_to_use, prompt)
#         values = [str(i) for i in range(1, 11)]
#         pattern = f"[{''.join(values)}]"
#         match = re.search(pattern, response)

#         return int(match.group(0)) if match else None

custom_metrics = [
    # CustomAnswerRelevancy()
]
