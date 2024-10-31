from abc import ABC, abstractmethod

# Python 3.8 type hints
from typing import Dict, Union, Optional

from src.prompts import *
from src.snowflake_utils import run_async_sql_complete


class Metric(ABC):
    def __init__(
        self,
        name: str,
        description: str,
        prompt: str,
        required: Dict[str, str],
    ):
        self.name = name
        self.description = description
        self.required = required
        self.session = None
        self.prompt = prompt

    @abstractmethod
    def get_prompt(self, *args):
        """Used to run any necessary data prep and impute prompt before LLM."""
        pass

    @abstractmethod
    def evaluate(self, *args):
        """Conducts the final LLM-as-a-judge prompt."""
        pass


# Cortex Analyst Metrics
class SQLResultsAccuracy(Metric):
    def __init__(
        self,
        model: str = "reka-flash"
    ):
        super().__init__(
            name="SQL Results Accuracy",
            description="""
Evaluates if 2 SQL queries return the same data given a user question.
Results are True or False.
Questions are best designed when expected results have less than 100 rows.""",
            prompt=SQLAccuracy_prompt,
            required={
                "question": "User question",
                "inference_sql": "LLM-generated SQL statement",
                "expected_sql": "Ground truth SQL statement",
            },
        )
        self.model = model

    def get_prompt(
        self, question: str, inference_sql: str, expected_sql: str
    ) -> Union[str, None]:
        if self.prompt is not None:
            from src.snowflake_utils import return_sql_result

            inference_data = return_sql_result(self.session, inference_sql)
            expected_data = return_sql_result(self.session, expected_sql)

            fstrings = {
                "question": question,
                "inference_data": inference_data,
                "expected_data": expected_data,
            }
            return self.prompt.format(**fstrings)
        else:
            return None

    def evaluate(
        self,
        question: str,
        inference_sql: str,
        expected_sql: str,
        model: Optional[str] = None,
    ):
        
        model_to_use = model if model else self.model  

        prompt = self.get_prompt(question, inference_sql, expected_sql)

        response = run_async_sql_complete(self.session, model_to_use, prompt)
        if "true" in response.lower():
            return True
        else:
            return False


# Knowledge Retrieval/Answer Metrics
class Correctness(Metric):
    def __init__(
        self,
        model: str = "llama3.1-8b"
    ):
        super().__init__(
            name="Correctness",
            description="""
Evaluates the correctness of a response compared to a reference answer on a scale of 1-5.
5 indicates the scorer strongly agrees that the response is correct and 1 indicates strong disagreement.""",
            prompt=Correctness_prompt,
            required={
                "question": "User question",
                "answer_ref": "Expected answer to the question",
                "ai_response": "LLM-generated response to the question",
            },
            
        )
        self.model = model

    def get_prompt(
        self, question: str, answer_ref: str, ai_response: str
    ) -> Union[str, None]:
        if self.prompt is not None:
            fstrings = {
                "question": question,
                "answer_ref": answer_ref,
                "ai_response": ai_response,
            }
            return self.prompt.format(**fstrings)
        else:
            return None

    def evaluate(
        self,
        question: str,
        answer_ref: str,
        ai_response: str,
        model: Optional[str] = None,
    ):
        import re

        model_to_use = model if model else self.model  
        
        prompt = self.get_prompt(question, answer_ref, ai_response)

        response = run_async_sql_complete(self.session, model_to_use, prompt)
        values = [str(i) for i in range(1, 11)]
        pattern = f"[{''.join(values)}]"
        match = re.search(pattern, response)

        return int(match.group(0)) if match else None


class Comprehensiveness(Metric):
    def __init__(
        self,
        model: str = "llama3.1-8b"
    ):
        super().__init__(
            name="Comprehensiveness",
            description="""
Evaluates the thoroughness and comprehensiveness of a response compared to a reference answer on a scale of 1-5.
5 indicates the scorer strongly agrees that the response is thorough and comprehensive and 1 indicates strong disagreement.""",
            prompt=Comprehensiveness_prompt,
            required={
                "question": "User question",
                "answer_ref": "Expected answer to the question",
                "ai_response": "LLM-generated response to the question",
            },
        )
        self.model = model

    def get_prompt(
        self, question: str, answer_ref: str, ai_response: str
    ) -> Union[str, None]:
        if self.prompt is not None:
            fstrings = {
                "question": question,
                "answer_ref": answer_ref,
                "ai_response": ai_response,
            }
            return self.prompt.format(**fstrings)
        else:
            return None

    def evaluate(
        self,
        question: str,
        answer_ref: str,
        ai_response: str,
        model: Optional[str] = None,
    ):
        import re

        model_to_use = model if model else self.model 

        prompt = self.get_prompt(question, answer_ref, ai_response)

        response = run_async_sql_complete(self.session, model_to_use, prompt)
        values = [str(i) for i in range(1, 11)]
        pattern = f"[{''.join(values)}]"
        match = re.search(pattern, response)

        return int(match.group(0)) if match else None


class Hallucination(Metric):
    def __init__(
        self,
        model: str = "llama3.1-8b"
    ):
        super().__init__(
            name="Hallucination",
            description="""
Evaluates the prevalance of hallucination in a response based on reference context on a scale of 1-5.
5 indicates the scorer strongly agrees that the response is hallucination-free and 1 indicates strong disagreement.""",
            prompt=Hallucination_prompt,
            required={
                "question": "User question",
                "context": "Applicable knowledge base context",
                "ai_response": "LLM-generated response to the question",
            },
        )
        self.model = model

    def get_prompt(
        self, question: str, context: str, ai_response: str
    ) -> Union[str, None]:
        if self.prompt is not None:
            fstrings = {
                "question": question,
                "context": context,
                "ai_response": ai_response,
            }
            return self.prompt.format(**fstrings)
        else:
            return None

    def evaluate(
        self, question: str, context: str, ai_response: str, model: Optional[str] = None,
    ):
        import re

        model_to_use = model if model else self.model  

        prompt = self.get_prompt(question, context, ai_response)

        response = run_async_sql_complete(self.session, model_to_use, prompt)
        values = [str(i) for i in range(1, 11)]
        pattern = f"[{''.join(values)}]"
        match = re.search(pattern, response)

        return int(match.group(0)) if match else None


# Non-Reference Prompt Metrics
class ConversationCohesiveness(Metric):
    def __init__(
        self,
        model: str = "llama3.1-8b"
    ):
        super().__init__(
            name="Conversation Cohesiveness",
            description="""
Evaluates the cohesivenss and adherence to topics of AI responses in conversation on a scale of 1-5.
5 indicates the scorer strongly agrees that the conversation is cohesive and stays on topic and 1 indicates strong disagreement.""",
            prompt=ConversationCohesiveness_prompt,
            required={
                "exchange": "Conversation between user and AI",
            },
        )
        self.model = model

    def get_prompt(self, exchange: str) -> Union[str, None]:
        if self.prompt is not None:
            fstrings = {
                "exchange": exchange,
            }
            return self.prompt.format(**fstrings)
        else:
            return None

    def evaluate(
        self,
        exchange: str,
        model: Optional[str] = None,
    ):
        import re

        model_to_use = model if model else self.model 

        prompt = self.get_prompt(exchange)

        response = run_async_sql_complete(self.session, model_to_use, prompt)
        values = [str(i) for i in range(1, 11)]
        pattern = f"[{''.join(values)}]"
        match = re.search(pattern, response)

        return int(match.group(0)) if match else None
    
class AnswerRelevancy(Metric):
    def __init__(
        self,
        model: str = "llama3.1-8b"
    ):
        super().__init__(
            name="Answer Relevancy",
            description="""
Evaluates the relevance of a response to a user question on a scale of 1-5.
5 indicates the scorer strongly agrees that the response is relevant and 1 indicates strong disagreement.""",
            prompt=AnswerRelevancy_prompt,
            required={
                "question": "User question",
                "ai_response": "LLM-generated response to the question",
            },
        )
        self.model = model

    def get_prompt(
        self, question: str, ai_response: str
    ) -> Union[str, None]:
        if self.prompt is not None:
            fstrings = {
                "question": question,
                "ai_response": ai_response,
            }
            return self.prompt.format(**fstrings)
        else:
            return None

    def evaluate(
        self,
        question: str,
        ai_response: str,
        model: Optional[str] = None,
    ):
        import re

        model_to_use = model if model else self.model 

        prompt = self.get_prompt(question, ai_response)

        response = run_async_sql_complete(self.session, model_to_use, prompt)
        values = [str(i) for i in range(1, 11)]
        pattern = f"[{''.join(values)}]"
        match = re.search(pattern, response)

        return int(match.group(0)) if match else None
    
class ContextualRelevancy(Metric):
    def __init__(
        self,
        model: str = "llama3.1-8b"
    ):
        super().__init__(
            name="Contextual Relevancy",
            description="""
Evaluates the contextual relevance of retrieved content in response to a user question on a scale of 1-5.
5 indicates the scorer strongly agrees that the response is contextually relevant and 1 indicates strong disagreement.""",
            prompt=ContextualRelevancy_prompt,
            required={
                "question": "User question",
                "retrieved_content": "Retrieved content in response to the question",
            },
        )
        self.model = model

    def get_prompt(
        self, question: str, retrieved_content: str
    ) -> Union[str, None]:
        if self.prompt is not None:
            fstrings = {
                "question": question,
                "retrieved_content": retrieved_content,
            }
            return self.prompt.format(**fstrings)
        else:
            return None

    def evaluate(
        self,
        question: str,
        retrieved_content: str,
        model: Optional[str] = None,
    ):
        import re

        model_to_use = model if model else self.model  

        prompt = self.get_prompt(question, retrieved_content)

        response = run_async_sql_complete(self.session, model_to_use, prompt)
        values = [str(i) for i in range(1, 11)]
        pattern = f"[{''.join(values)}]"
        match = re.search(pattern, response)

        return int(match.group(0)) if match else None


# Metric categorization for display
cortex_analyst_metrics = {
    "section_name": "Cortex Analyst Metrics",
    "caption": """Suggested metrics to evaluate the performance of Cortex Analyst SQL generation.""",
    "metrics": [
        SQLResultsAccuracy(),
    ],
}
knowledge_base_retrieval_metrics = {
    "section_name": "Knowledge-Based Reference Metrics",
    "caption": """Suggested metrics to evaluate the quality of knowledge-based responses given reference material.""",
    "metrics": [
        Correctness(),
        Comprehensiveness(),
        Hallucination(),
    ],
}
non_knowledge_base_retrieval_metrics = {
    "section_name": "Non-Knowledge-Based Reference Metrics",
    "caption": """Suggested metrics to evaluate the quality of responses without reference material.""",
    "metrics": [
        ConversationCohesiveness(),
        AnswerRelevancy(),
        ContextualRelevancy()
    ],
}

# All metrics
metrics = [
    SQLResultsAccuracy(),
    Correctness(),
    Comprehensiveness(),
    Hallucination(),
    ConversationCohesiveness(),
    AnswerRelevancy(),
    ContextualRelevancy()
]

# Display metrics on homepage by section
metric_display = [
    cortex_analyst_metrics,
    knowledge_base_retrieval_metrics,
    non_knowledge_base_retrieval_metrics,
]
