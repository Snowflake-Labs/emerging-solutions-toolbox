SQLAccuracy_prompt = """You are evaluating JSON data against ground truth JSON data.
The JSON data is the output of a SQL query generated to answer a user question.
You are to determine if the provided JSON data matches the ground truth JSON data
and answers the user question.
The Inference JSON does not have to match the Ground Truth JSON perfectly but should contain the correct answer as denoted by the Ground Truth JSON.
Your answer should be either "True" or "False".
Answer "True" if you believe the Inference JSON data reflects the Ground Truth JSON data given the user question.
Otherwise, answer "False".
[User Question]
{question}

[The Start of the Inference JSON Data]
{inference_data}
[The End of the Inference JSON Data]

[The Start of the Ground Truth Data]
{expected_data}
[The End of the Ground Truth Data]
"""

## Benchmark prompt used in internal Cortex Analyst benchmarking - under team review
# SQLAccuracy_prompt = """\
# [INST] Your task is to determine whether the two given JSON datasets are
# equivalent semantically in the context of a question. You should attempt to
# answer the given question by using the data in each JSON dataset. If the two
# answers are equivalent, those two JSON datasets are considered equivalent.
# Otherwise, they are not equivalent.
# If they are equivalent, output "ANSWER: true". If they are
# not equivalent, output "ANSWER: false".

# ### QUESTION: {question}

# * JSON DATASET 1:
# {inference_data}

# * DATAFRAME 2:
# {expected_data}

# Are the two dataframes equivalent?
# OUTPUT:
# [/INST] """

Correctness_prompt = """Please act as an impartial judge and evaluate the quality of the response provided by the AI Assistant to the user question displayed below.
Your evaluation should consider CORRECTNESS. You will be given a reference answer and the AI Assistant's answer.
Your job is to rate the assistant's answer from 1 to 5, where 5 indicates you strongly agree that the response is
CORRECT
and 1 indicates you strongly disagree.
Avoid any position biases and ensure that the order in which the content presented does not affect your evaluation.
Be as objective as possible. Output your rating with just the number rating value.
[User Question]
{question}

[The Start of the Reference Answer]
{answer_ref}
[The End of the Reference Answer]

[The Start of the AI Assistant's Answer]
{ai_response}
[The End of the AI Assistant's Answer]
"""

Comprehensiveness_prompt = """Please act as an impartial judge and evaluate the quality of the response provided by the AI Assistant to the user question displayed below.
Your evaluation should consider THOROUGHNESS and COMPREHENSIVENESS. You will be given a reference answer and the AI Assistant's answer.
Your job is to rate the assistant's response from 1 to 5, where 5 indicates you strongly agree that the response is
THOROUGH and COMPREHENSIVE
and 1 indicates you strongly disagree.
Avoid any position biases and ensure that the order in which the content presented does not affect your evaluation.
Be as objective as possible. Output your rating with just the number rating value.
[User Question]
{question}

[The Start of the Reference Answer]
{answer_ref}
[The End of the Reference Answer]

[The Start of the AI Assistant's Answer]
{ai_response}
[The End of the AI Assistant's Answer]
"""

Hallucination_prompt = """Please act as an impartial judge and evaluate the prevalance of hallucination in the response provided by the AI Assistant to the user question displayed below.
Your evaluation should consider only context provided in the reference material. You will be given reference content and the AI Assistant's response.
Your job is to rate the assistant's response from 1 to 5, where 5 indicates you strongly agree that the response is
HALLUCINATION-FREE
and 1 indicates you strongly disagree.
Avoid any position biases and ensure that the order in which the content presented does not affect your evaluation.
Be as objective as possible. Output your rating with just the number rating value.
[User Question]
{question}

[Reference Material]
{context}

[The Start of the AI Assistant's Response]
{ai_response}
[The End of the AI Assistant's Response]
"""

ConversationCohesiveness_prompt = """Please act as an impartial judge and evaluate the quality of the response(s) provided by the AI Assistant to the user question(s).
Your evaluation should consider COHESIVENESS and the degree to which the AI Assistant stays on topic in conversation. You will be given both user queries and the AI Assistant response(s).
Your job is to rate the assistant's conversation from 1 to 5, where 5 indicates you strongly agree that the assistant's response(s) were
COHESIVE and ON TOPIC
and 1 indicates you strongly disagree.
Avoid any position biases and ensure that the order in which the content presented does not affect your evaluation.
Be as objective as possible. Output your rating with just the number rating value.

[The Start of the User and AI Exchange]
{exchange}
[The End of the User and AI Exchange]
"""

AnswerRelevancy_prompt = """Please act as an impartial judge and evaluate the quality of the response provided by the AI Assistant to the user question displayed below.
Your evaluation should consider RELEVANCY. You will be given a user question and the AI Assistant's answer.
Your job is to rate the assistant's response from 1 to 5, where 5 indicates you strongly agree that the response is
RELEVANT
and 1 indicates you strongly disagree.
Avoid any position biases and ensure that the order in which the content presented does not affect your evaluation.
Be as objective as possible. Output your rating with just the number rating value.
[User Question]
{question}

[The Start of the AI Assistant's Answer]
{ai_response}
[The End of the AI Assistant's Answer]
"""

ContextualRelevancy_prompt = """Please act as an impartial judge and evaluate the quality of the retrieved content provided by a content retrieval mechanism in response to the user question displayed below.
Your evaluation should consider CONTEXTUAL RELEVANCY. You will be given a user question and the retrieved content.
Your job is to rate the assistant's response from 1 to 5, where 5 indicates you strongly agree that the retrieved content is
RELEVANT
and 1 indicates you strongly disagree.
Avoid any position biases and ensure that the order in which the content presented does not affect your evaluation.
Be as objective as possible. Output your rating with just the number rating value.
[User Question]
{question}

[The Start of the Retrieved Content]
{retrieved_content}
[The End of the Retrieved Content]
"""

Recommendation_prompt = """You're an AI Assistant tasked with helping an analyst improve their generative AI evaluation results that use LLM-as-a-judge.
You will receive a single record from the generative AI evaluation results table that includes
the LLM-as-a-judge prompt and generated score.
Concisely explain to the user why the LLM-as-a-judge score is what it is and what changes can be done to the inputs to improve it if the score is not perfect.
Refer to the LLM-as-a-judge as the Scorer.
Be concise and only comment on this particular record.

[The Start of the LLM-as-a-judge Prompt]
{prompt}
[The End of the LLM-as-a-judge Prompt]

[The LLM-as-a-judge Score]
{score}
"""
