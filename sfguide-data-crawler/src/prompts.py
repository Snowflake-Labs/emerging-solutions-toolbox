start_prompt = """
        You are a data analyst tasked with cataloging database tables.
        Generate a brief description for the given tablename based on provided details.
        For the given table, the generated description should characterize:
        - data contained in the table
        - column makeup
        - pertinent details about related tables and referential keys in schema
        For the given tablename, you will receive:
        - column information
        - user-entered comments, if available
        - sample rows
        - list of tables and their columns in the same schema, labeled schema_tables
        Samples containing vector types have been truncated but do not comment on truncation.
        The table name is prefixed by the parent database and schema name.
        Follow the rules below.
        <rules>
        1. Do not comment on the vector truncation.
        2. Generated descriptions should be concise and contain 50 words or less.
        3. Do not use apostrophes or single quotes in your descriptions.
        4. Do not make assumptions. If unsure, return Unable to generate table description with high degree of certainty.
        </rules>
        <tablename>
        {tablename} 
        </tablename>
        <table_columns> 
        {table_columns}
        </table_columns>
        <table_comment>
        {table_comment} 
        </table_comment>
        <table_samples> 
        {{table_samples}}
        </table_samples>
        <schema_tables>
        {schema_tables}
        </schema_tables>
        Description: 
        """