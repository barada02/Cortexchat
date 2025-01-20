# Build

## **1.Create a database and a schema**

Run the following code inside your newly created worksheet

```
CREATE DATABASE Cortex_Quickstart_Chatbot;
CREATE SCHEMA MAIN;
```

### **2.Create a table function that will split text into chunks**

We will be using the Langchain Python library to accomplish the necessary document split tasks. Because as part of Snowpark Python these are available inside the integrated Anaconda repository, there are no manual installs or Python environment and dependency management required.

Relevant documentation:

- [Using third-party libraries in Snowflake](https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-packages)
- [Python User Defined Table Function](https://docs.snowflake.com/en/developer-guide/snowpark/python/creating-udtfs)

Create the function by [running the following query](https://docs.snowflake.com/en/user-guide/ui-snowsight-query#executing-and-running-queries) inside your SQL worksheet

```
create or replace function text_chunker(pdf_text string)
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
```

3.Create a Stage with Directory Table where you will be uploading your documents

```
create or replace stage cdocs ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE') DIRECTORY = ( ENABLE = true );
```

### *4.Upload documents to your staging area*

- Select **Data** on the left of Snowsight
- Click on your database **Cortex_Quickstart_Chatbot**
- Click on your schema **MAIN**
- Click on **Stages** and select **CDocs**
- On the top right click on the +Files botton
- Drag and drop the two PDF files you downloaded

![https://quickstarts.snowflake.com/guide/ask_questions_to_your_own_documents_with_snowflake_cortex_search/img/7a4b5b58f8a330f0.png](https://quickstarts.snowflake.com/guide/ask_questions_to_your_own_documents_with_snowflake_cortex_search/img/7a4b5b58f8a330f0.png)

### ***5. Check files has been successfully uploaded***

```
ls @cdocs;
```

### ***6.Create the table where we are going to store the chunks for each PDF.***

```
create or replace TABLE CDOCS_CHUNKS_TABLE (
    RELATIVE_PATH VARCHAR(16777216), -- Relative path to the PDF file
    SIZE NUMBER(38,0), -- Size of the PDF
    FILE_URL VARCHAR(16777216), -- URL for the PDF
    SCOPED_FILE_URL VARCHAR(16777216), -- Scoped url (you can choose which one to keep depending on your use case)
    CHUNK VARCHAR(16777216), -- Piece of text
    CATEGORY VARCHAR(16777216) -- Will hold the document category to enable filtering
);
```

### ***7.Text Parsing and chunking Operation***

 Function [SNOWFLAKE.CORTEX.PARSE_DOCUMENT](https://docs.snowflake.com/en/sql-reference/functions/parse_document-snowflake-cortex) will be used to read the PDF documents directly from the staging area. The text will be passed to the function previously created to split the text into chunks. There is no need to create embeddings as that will be managed automatically by Cortex Search service later.

```
insert into cdocs_chunks_table (relative_path, size, file_url,
                            scoped_file_url, chunk)

    select relative_path,
            size,
            file_url,
            build_scoped_file_url(@cdocs, relative_path) as scoped_file_url,
            func.chunk as chunk
    from
        directory(@cdocs),
        TABLE(text_chunker (TO_VARCHAR(SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@cdocs,
                              relative_path, {'mode': 'LAYOUT'})))) as func;
```

### ***8.Create Cortex Search Service***

Next step is to create the CORTEX SEARCH SERVICE in the table we have created before. We will execute this SQL command:

```
create or replace CORTEX SEARCH SERVICE CORTEX_CHAT_SEARCH_SERVICE_CS
ON chunk
ATTRIBUTES category
warehouse = COMPUTE_WH
TARGET_LAG = '1 minute'
as (
    select chunk,
        relative_path,
        file_url,
        category
    from CDOCS_CHUNKS_TABLE
);
```

- The name of the service is **CORTEX_CHAT_SEARCH_SERVICE_CS**
- The service will use the column **chunk** to create embeddings and perform retrieval based on similarity search
- The column **category** could be used as a filter
- To keep this service updated the warehosue **COMPUTE_WH** will be used. This name is used by default in trial accounts but you may want to type the name of your own warehouse.
- The service will be refreshed every minute
- The data retrieved will contain the **chunk, relative_path, file_url and category**

This is all what we have to do. There is no need here to create embeddings as that is done automatically. We can now use the API to query the service.