#!/usr/bin/env python3

# Adapted from: Tutorial 3: Build a PDF chatbot with Cortex Search
# https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/tutorials/cortex-search-tutorial-3-chat-advanced

import io

import pandas as pd
import PyPDF2
from langchain.text_splitter import RecursiveCharacterTextSplitter
from snowflake.connector import connect
from snowflake.core import Root
from snowflake.core.database import Database
from snowflake.core.schema import Schema
from snowflake.core.stage import Stage, StageDirectoryTable, StageEncryption
from snowflake.core.table import Table, TableCollection, TableColumn
from snowflake.core.warehouse import Warehouse
from snowflake.snowpark import Session
from snowflake.snowpark import functions as F
from snowflake.snowpark import types as T
from snowflake.snowpark.files import SnowflakeFile

con = connect()
root = Root(con)
session = Session.builder.config("connection", con).create()

root.warehouses.create(
    Warehouse(
        name="CORTEX_SEARCH_TUTORIAL_WH",
        warehouse_size="X-SMALL",
        auto_suspend=120,
        auto_resume="true",
        initially_suspended="true",
    ),
    mode="orReplace",
)

root.databases.create(
    Database(name="CORTEX_SEARCH_TUTORIAL_DB", kind="PERMANENT"), mode="orReplace"
)

database = root.databases["CORTEX_SEARCH_TUTORIAL_DB"]

database.schemas.create(Schema(name="OTHER"))
session.use_schema("PUBLIC")

schema = database.schemas["PUBLIC"]

schema.stages.create(
    Stage(
        name="FOMC",
        directory_table=StageDirectoryTable(enable=True),
        encryption=StageEncryption(type="SNOWFLAKE_SSE"),
    ),
    mode="orReplace",
)

schema.stages.create(
    Stage(name="PYTHON_CODE"),
    mode="orReplace",
)

schema.stages["FOMC"].put(
    stage_location="/",
    local_file_name="data/*.pdf",
    auto_compress=False,
    overwrite=True,
)

## TODO: Is there a way to use the Core API to refresh the stage after PUT?
con.cursor().execute("ALTER STAGE FOMC REFRESH")

tc = TableCollection(schema=schema)

parse_docs_chunks_ocr_table = Table(
    name="PARSE_DOC_CHUNKS_OCR",
    mode="PERMANENT",
    columns=[
        TableColumn(name="STAGE_PATH", datatype="VARCHAR", nullable=False),
        TableColumn(name="RELATIVE_PATH", datatype="VARCHAR", nullable=False),
        TableColumn(name="LAST_MODIFIED", datatype="TIMESTAMP_TZ(3)", nullable=False),
        TableColumn(name="OCR_RESULTS", datatype="VARIANT", nullable=False),
    ],
    comment='{"origin":"sf_sit","name":"contracts_chatbot","version":{"major":1, "minor":0},"attributes":{"component":"Objects"}}',
)

pdfs_to_process_table = Table(
    name="PDFS_TO_PROCESS",
    mode="PERMANENT",
    columns=[
        TableColumn(name="RELATIVE_PATH", datatype="VARCHAR", nullable=False),
        TableColumn(name="LAST_MODIFIED", datatype="TIMESTAMP_TZ(3)", nullable=False),
        TableColumn(name="STAGE_PATH", datatype="VARCHAR", nullable=False),
        TableColumn(name="NUM_PAGES", datatype="INT", nullable=False),
    ],
    comment='{"origin":"sf_sit","name":"contracts_chatbot","version":{"major":1, "minor":0},"attributes":{"component":"Objects"}}',
)

parse_docs_chunks_ocr_chunked_out_table = Table(
    name="PARSE_DOC_CHUNKS_OCR_CHUNKED_OUT",
    mode="PERMANENT",
    columns=[
        TableColumn(name="STAGE_PATH", datatype="VARCHAR", nullable=False),
        TableColumn(name="RELATIVE_PATH", datatype="VARCHAR", nullable=False),
        TableColumn(name="LAST_MODIFIED", datatype="TIMESTAMP_TZ(3)", nullable=False),
        TableColumn(name="CHUNK", datatype="VARCHAR", nullable=False),
    ],
    comment='{"origin":"sf_sit","name":"contracts_chatbot","version":{"major":1, "minor":0},"attributes":{"component":"Objects"}}',
)

tc.create(parse_docs_chunks_ocr_table, mode="orReplace")
tc.create(pdfs_to_process_table, mode="orReplace")
tc.create(parse_docs_chunks_ocr_chunked_out_table, mode="orReplace")


def count_pdf_pages(file_url: str) -> int:
    with SnowflakeFile.open(file_url, "rb") as f:
        buffer = io.BytesIO(f.readall())

    reader = PyPDF2.PdfReader(buffer)

    return len(reader.pages)


session.udf.register(
    count_pdf_pages,
    return_type=T.LongType(),
    input_types=[T.StringType()],
    name="COUNT_PDF_PAGES",
    is_permanent=True,
    stage_location="PYTHON_CODE",
    packages=("snowflake-snowpark-python", "PyPDF2"),
    replace=True,
    comment='{"origin":"sf_sit","name":"contracts_chatbot","version":{"major":1, "minor":0},"attributes":{"component":"Count Pages Fn"}}',
)

(
    session.sql(
        "SELECT RELATIVE_PATH, LAST_MODIFIED FROM DIRECTORY(@FOMC)"
    ).with_columns(
        ["STAGE_PATH", "NUM_PAGES"],
        [
            F.concat_ws(
                F.lit("/"),
                F.concat(F.lit("@"), F.current_schema(), F.lit("."), F.lit("FOMC")),
                F.col("RELATIVE_PATH"),
            ),
            F.call_function(
                "COUNT_PDF_PAGES",
                F.call_builtin(
                    "BUILD_SCOPED_FILE_URL", "@FOMC", F.col("RELATIVE_PATH")
                ),
            ),
        ],
    )
).write.save_as_table(
    "PDFS_TO_PROCESS",
    mode="append",
)

parse_col_expr = F.call_builtin(
    "SNOWFLAKE.CORTEX.PARSE_DOCUMENT",
    F.lit("@FOMC"),
    F.col("RELATIVE_PATH"),
    F.lit({"mode": "OCR"}),
)

(
    (
        session.table("PDFS_TO_PROCESS")
        .filter(F.col("NUM_PAGES") <= 100)
        .join(
            session.table("PARSE_DOC_CHUNKS_OCR"),
            on=["RELATIVE_PATH", "LAST_MODIFIED"],
            how="anti",
        )
    )
    .select(
        "STAGE_PATH",
        "RELATIVE_PATH",
        "LAST_MODIFIED",
        parse_col_expr.alias("OCR_RESULTS"),
    )
    .write.save_as_table(
        "PARSE_DOC_CHUNKS_OCR",
        mode="append",
    )
)


class PdfTextSplitter:
    def process(self, input_string: str):
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=2000,
            chunk_overlap=300,
            length_function=len,
        )
        chunks = text_splitter.split_text(input_string)
        df = pd.DataFrame(chunks, columns=["chunk"])
        yield from df.itertuples(index=False, name=None)


session.udtf.register(
    PdfTextSplitter,
    output_schema=T.StructType([T.StructField("CHUNK", T.StringType())]),
    input_types=[T.StringType()],
    input_names=["INPUT_STRING"],
    name="PDF_TEXT_SPLITTER",
    is_permanent=True,
    stage_location="PYTHON_CODE",
    packages=("snowflake-snowpark-python", "langchain", "pandas"),
    replace=True,
    comment='{"origin":"sf_sit","name":"contracts_chatbot","version":{"major":1, "minor":0},"attributes":{"component":"Text Splitter Fn"}}',
)

(
    session.table("PARSE_DOC_CHUNKS_OCR")
    .join_table_function(
        "PDF_TEXT_SPLITTER",
        F.cast(F.col("OCR_RESULTS")["content"], T.StringType()),
    )
    .drop("OCR_RESULTS")
).write.save_as_table(
    "PARSE_DOC_CHUNKS_OCR_CHUNKED_OUT",
    mode="append",
)

# TODO: Is it possible to create this programmatically with the Core API?
session.sql(
    """CREATE OR REPLACE CORTEX SEARCH SERVICE cortex_search_tutorial_db.public.fomc_meeting
    ON chunk
    WAREHOUSE = cortex_search_tutorial_wh
    TARGET_LAG = '1 hour'
    COMMENT = '{"origin":"sf_sit","name":"contracts_chatbot","version":{"major":1, "minor":0},"attributes":{"component":"cortex search service"}}'
    AS (
    SELECT
        chunk,
        STAGE_PATH,
        relative_path
    FROM PARSE_DOC_CHUNKS_OCR_CHUNKED_OUT
    );"""
).show()
