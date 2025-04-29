from snowflake.cortex import CompleteOptions

class TableauCalculationPrompt():
    def __init__(self,
                 alias,
                 formula,) -> None:
        self.system_prompt = f"""You are an Analyst tasked with translating a Tableau data source into a semantic model for Snowflake.
        You will be asked to translate a Tableau calculated field to the SQL equivalent for Snowflake.
        You will receive the following for the Tableau calculation:
        - field name
        - calculation formula

        You are to translate the Tableau formula to a Snowflake SQL SELECT clause
        AND determine if the field is pertinent for a semantic model.
        The formula itself may suggest if the field is just for aesthetics in Tableau vs. a field that has value for a semantic model.

        The Tableau formula contains column references enclosed with brackets like [<table_name>.<column_name>].
        If the Tableau formula cannot be translated, provide an explanation.
        Follow these rules when translating the formula:
        - Retain the Tableau formula column references exactly as shown in the formula but remove the brackets. Do not omit any part of the column reference.
        - The Snowflake SQL formula should be a valid Snowflake SQL SELECT expression. But omit the SELECT keyword in the Snowflake SQL formula.
        - The Snowflake SQL formula must be syntactically correct to use in a Snowflake SELECT clause like 'SELECT <formula>'.
        - If the Tableau formula cannot be translated, provide an explanation and omit the translated formula in the response.
        - If the formula and name are not relevant for a semantic model, omit the translated formula in the response.

        Mark the calculation as valid = False and provide an explanation if it is not pertinent or relevant for a semantic model."""
        self.user_prompt = f"""Here is the Tableau calculated field information:
        Field Name: {alias}
        Formula: {formula}"""
        self.response_schema = {
            "type": "json",
            "schema": {
                "type": "object",
                "properties": {
                    "valid": {"type": "boolean",
                              "description": "true or false indicating whether the formula can be translated to Snowflake SQL AND if it's pertinent to a semantic model."},
                    "translated_formula": {"type": "string",
                              "description": "The translated formula for the semantic model."},
                    "explanation": {"type": "string",
                              "description": "Explanation if the formula cannot be translated. Otherwise, empty string."},
                },
                "required": [
                    "valid",
                    "explanation",
                ],
            }
        }
        self.model = 'claude-3-5-sonnet'
        self.temperature = 0.0
        self.max_tokens = 1000
        self.prompt=[
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": self.user_prompt}
            ]
        self.options = CompleteOptions(
                max_tokens=self.max_tokens,
                temperature=self.temperature,
                response_format=self.response_schema
            )
