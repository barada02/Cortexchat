import snowflake.connector
from snowflake.snowpark import Session
import streamlit as st

# Function to establish a connection to Snowflake

def connect_to_snowflake():
    connection_parameters = {
        "account": st.secrets["connections"]["snowflake"]["account"],
        "user": st.secrets["connections"]["snowflake"]["user"],
        "password": st.secrets["connections"]["snowflake"]["password"],
        "role": st.secrets["connections"]["snowflake"]["role"],
        "warehouse": st.secrets["connections"]["snowflake"]["warehouse"],
        "database": st.secrets["connections"]["snowflake"]["database"],
        "schema": st.secrets["connections"]["snowflake"]["schema"]
    }
    session = Session.builder.configs(connection_parameters).create()
    return session

# Function to check and create database, schema, stage, and table

def setup_environment(session):
    try:
        # Check if database exists
        session.sql("CREATE DATABASE IF NOT EXISTS Cortex_Quickstart_Chatbot;").collect()
        # Check if schema exists
        session.sql("CREATE SCHEMA IF NOT EXISTS MAIN;").collect()
        # Check if stage exists
        session.sql("CREATE OR REPLACE STAGE cdocs ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE') DIRECTORY = (ENABLE = true);").collect()
        # Check if table exists
        session.sql("""CREATE OR REPLACE TABLE CDOCS_CHUNKS_TABLE (
            RELATIVE_PATH VARCHAR(16777216),
            SIZE NUMBER(38,0),
            FILE_URL VARCHAR(16777216),
            SCOPED_FILE_URL VARCHAR(16777216),
            CHUNK VARCHAR(16777216),
            CATEGORY VARCHAR(16777216)
        );""").collect()
        create_text_chunker_function(session)
        create_cortex_search_service(session)
    except Exception as e:
        st.error(f"An error occurred during setup: {e}")

def create_text_chunker_function(session):
    try:
        session.sql("""create or replace function text_chunker(pdf_text string)
        returns table (chunk varchar)
        language python
        runtime_version = '3.9'
        handler = 'text_chunker'
        packages = ('snowflake-snowpark-python', 'langchain')
        as
        $$
        from snowflake.snowpark.types import StringType, StructField, StructType
        from langchain.text_splitter import RecursiveCharacterTextSplitter
        import pandas as pd

        class text_chunker:
            
            def process(self, pdf_text: str):
                
                text_splitter = RecursiveCharacterTextSplitter(
                    chunk_size = 1512,#Adjust this as you see fit
                    chunk_overlap  = 256,#This let's text have some form of overlap. Useful for keeping chunks contextual
                    length_function = len
        )

        chunks = text_splitter.split_text(pdf_text)
        df = pd.DataFrame(chunks, columns=['chunks'])
        yield from df.itertuples(index=False, name=None)
        $$;
        """).collect()
    except Exception as e:
        st.error(f"An error occurred while creating text_chunker function: {e}")

def create_cortex_search_service(session):
    try:
        session.sql("""
            CREATE OR REPLACE CORTEX SEARCH SERVICE CORTEX_CHAT_SEARCH_SERVICE_CS
            ON chunk
            ATTRIBUTES category
            WAREHOUSE = COMPUTE_WH
            TARGET_LAG = '1 minute'
            AS (
                SELECT chunk,
                    relative_path,
                    file_url,
                    category
                FROM CDOCS_CHUNKS_TABLE
            );
        """).collect()
    except Exception as e:
        st.error(f"An error occurred while creating Cortex Search Service: {e}")

# Function to process uploaded file

def process_uploaded_file(uploaded_file, session):
    try:
        # Read the uploaded file
        pdf_text = uploaded_file.read()

        # Insert into the Snowflake stage
        session.file.put(pdf_text, '@cdocs')

        # Execute the text parsing and chunking operation
        session.sql("""
            INSERT INTO CDOCS_CHUNKS_TABLE (relative_path, size, file_url, scoped_file_url, chunk)
            SELECT relative_path, size, file_url, build_scoped_file_url(@cdocs, relative_path) as scoped_file_url, func.chunk as chunk
            FROM directory(@cdocs), TABLE(text_chunker (TO_VARCHAR(SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@cdocs, relative_path, {'mode': 'LAYOUT'})))) as func;
        """).collect()

        st.success("Document processed successfully!")
    except Exception as e:
        st.error(f"An error occurred while processing the document: {e}")

# Function to initialize the environment

def initialize_environment():
    session = connect_to_snowflake()
    setup_environment(session)
