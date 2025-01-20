import streamlit as st

# Set the page configuration first
st.set_page_config(layout="wide", page_title="Document Chat Assistant")

# Import other necessary modules
from setup import initialize_environment, process_uploaded_file
from snowflake.snowpark import Session
from snowflake.cortex import Complete
from snowflake.core import Root
import snowflake.connector

import pandas as pd
import json

pd.set_option("max_colwidth",None)

# Call the setup function when the app loads
initialize_environment()

### Default Values
NUM_CHUNKS = 3 # Num-chunks provided as context. Play with this to check how it affects your accuracy
slide_window = 7 # how many last conversations to remember. This is the slide window.

# Retrieve service parameters from secrets.toml
CORTEX_SEARCH_DATABASE = st.secrets["connections"]["snowflake"]["database"]
CORTEX_SEARCH_SCHEMA = st.secrets["connections"]["snowflake"]["schema"]
CORTEX_SEARCH_SERVICE = st.secrets["connections"]["snowflake"]["cortex_search_service"]

# columns to query in the service
COLUMNS = [
    "chunk",
    "relative_path",
    "category"
]

# Establish a connection to Snowflake using Streamlit's secrets
try:
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
    root = Root(session)
    
    svc = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[CORTEX_SEARCH_SERVICE]
except snowflake.connector.errors.Error as e:
    st.error(f"Failed to connect to Snowflake: {e}")

### Functions
     
def config_options():
    try:
        st.sidebar.selectbox('Select your model:',(
                                        'mixtral-8x7b',
                                        'snowflake-arctic',
                                        'mistral-large',
                                        'llama3-8b',
                                        'llama3-70b',
                                        'reka-flash',
                                         'mistral-7b',
                                         'llama2-70b-chat',
                                         'gemma-7b'), key="model_name")

        categories = session.table('docs_chunks_table').select('category').distinct().collect()

        cat_list = ['ALL']
        for cat in categories:
            cat_list.append(cat.CATEGORY)
                
        st.sidebar.selectbox('Select what products you are looking for', cat_list, key = "category_value")

        st.sidebar.checkbox('Do you want that I remember the chat history?', key="use_chat_history", value = True)

        st.sidebar.checkbox('Debug: Click to see summary generated of previous conversation', key="debug", value = True)
        st.sidebar.button("Start Over", key="clear_conversation", on_click=init_messages)
        st.sidebar.expander("Session State").write(st.session_state)
    except Exception as e:
        st.error(f"An error occurred in config_options: {e}")

def init_messages():
    try:
        # Initialize chat history
        if st.session_state.clear_conversation or "messages" not in st.session_state:
            st.session_state.messages = []
    except Exception as e:
        st.error(f"An error occurred in init_messages: {e}")

def get_similar_chunks_search_service(query):
    try:
        if st.session_state.category_value == "ALL":
            response = svc.search(query, COLUMNS, limit=NUM_CHUNKS)
        else: 
            filter_obj = {"@eq": {"category": st.session_state.category_value} }
            response = svc.search(query, COLUMNS, filter=filter_obj, limit=NUM_CHUNKS)

        st.sidebar.json(response.json())
        
        return response.json()  
    except Exception as e:
        st.error(f"An error occurred in get_similar_chunks_search_service: {e}")

def get_chat_history():
    try:
    #Get the history from the st.session_stage.messages according to the slide window parameter
        
        chat_history = []
        
        start_index = max(0, len(st.session_state.messages) - slide_window)
        for i in range (start_index , len(st.session_state.messages) -1):
             chat_history.append(st.session_state.messages[i])

        return chat_history
    except Exception as e:
        st.error(f"An error occurred in get_chat_history: {e}")

def summarize_question_with_history(chat_history, question):
    try:
    # To get the right context, use the LLM to first summarize the previous conversation
    # This will be used to get embeddings and find similar chunks in the docs for context

        prompt = f"""
            Based on the chat history below and the question, generate a query that extend the question
            with the chat history provided. The query should be in natual language. 
            Answer with only the query. Do not add any explanation.
            
            <chat_history>
            {chat_history}
            </chat_history>
            <question>
            {question}
            </question>
            """
        
        sumary = Complete(st.session_state.model_name, prompt)   

        if st.session_state.debug:
            st.sidebar.text("Summary to be used to find similar chunks in the docs:")
            st.sidebar.caption(sumary)

        sumary = sumary.replace("'", "")

        return sumary
    except Exception as e:
        st.error(f"An error occurred in summarize_question_with_history: {e}")

def create_prompt (myquestion):
    try:
        if st.session_state.use_chat_history:
            chat_history = get_chat_history()

            if chat_history != []: #There is chat_history, so not first question
                question_summary = summarize_question_with_history(chat_history, myquestion)
                prompt_context =  get_similar_chunks_search_service(question_summary)
            else:
                prompt_context = get_similar_chunks_search_service(myquestion) #First question when using history
        else:
            prompt_context = get_similar_chunks_search_service(myquestion)
            chat_history = ""
      
        prompt = f"""
               You are an expert chat assistance that extracs information from the CONTEXT provided
               between <context> and </context> tags.
               You offer a chat experience considering the information included in the CHAT HISTORY
               provided between <chat_history> and </chat_history> tags..
               When ansering the question contained between <question> and </question> tags
               be concise and do not hallucinate. 
               If you don´t have the information just say so.
               
               Do not mention the CONTEXT used in your answer.
               Do not mention the CHAT HISTORY used in your asnwer.

               Only anwer the question if you can extract it from the CONTEXT provideed.
               
               <chat_history>
               {chat_history}
               </chat_history>
               <context>          
               {prompt_context}
               </context>
               <question>  
               {myquestion}
               </question>
               Answer: 
               """
        
        json_data = json.loads(prompt_context)

        relative_paths = set(item['relative_path'] for item in json_data['results'])

        return prompt, relative_paths
    except Exception as e:
        st.error(f"An error occurred in create_prompt: {e}")

def answer_question(myquestion):
    try:
        prompt, relative_paths =create_prompt (myquestion)

        response = Complete(st.session_state.model_name, prompt)   

        return response, relative_paths
    except Exception as e:
        st.error(f"An error occurred in answer_question: {e}")

def main():
    try:
        uploaded_file = st.sidebar.file_uploader("Upload Document", type=["pdf"], key="upload")

        if uploaded_file is not None:
            process_uploaded_file(uploaded_file, session)
            st.success("File uploaded successfully!")
            # Trigger the data ingestion process here
            # (e.g., call a function to process the uploaded file)

        # Custom CSS for better styling
        st.markdown("""
            <style>
            .stApp {
                max-width: 1200px;
                margin: 0 auto;
            }
            .main-header {
                text-align: center;
                color: #0E1117;
                padding: 1.5rem 0;
                border-bottom: 2px solid #E0E0E0;
                margin-bottom: 2rem;
            }
            .document-section {
                background-color: #F8F9FA;
                padding: 1rem;
                border-radius: 10px;
                margin-bottom: 1.5rem;
            }
            </style>
        """, unsafe_allow_html=True)

        # Main header with emoji and title
        st.markdown("<div class='main-header'><h1>📚 Document Chat Assistant</h1><p>Powered by Snowflake Cortex</p></div>", unsafe_allow_html=True)
        
        # Document section with better styling
        st.markdown("<div class='document-section'>", unsafe_allow_html=True)
        st.subheader("📋 Available Documents")
        st.write("Here are the documents that I can help you with:")
        docs_available = session.sql("ls @docs").collect()
        list_docs = []
        for doc in docs_available:
            list_docs.append(doc["name"])
        st.dataframe(list_docs, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

        config_options()
        init_messages()
         
        # Chat interface with better styling
        st.markdown("<div style='margin-top: 2rem;'>", unsafe_allow_html=True)
        for message in st.session_state.messages:
            with st.chat_message(message["role"], avatar="🧑‍💻" if message["role"] == "user" else "🤖"):
                st.markdown(message["content"])
        
        # Chat input with custom placeholder
        if question := st.chat_input("Ask me anything about the documents..."):
            st.session_state.messages.append({"role": "user", "content": question})
            with st.chat_message("user", avatar="🧑‍💻"):
                st.markdown(question)
            
            with st.chat_message("assistant", avatar="🤖"):
                message_placeholder = st.empty()
                
                question = question.replace("'","")
                
                with st.spinner("🤔 Analyzing documents..."):
                    response, relative_paths = answer_question(question)            
                    response = response.replace("'", "")
                    message_placeholder.markdown(response)

                    if relative_paths != "None":
                        with st.sidebar.expander("📑 Related Documents"):
                            for path in relative_paths:
                                cmd2 = f"select GET_PRESIGNED_URL(@docs, '{path}', 360) as URL_LINK from directory(@docs)"
                                df_url_link = session.sql(cmd2).to_pandas()
                                url_link = df_url_link._get_value(0,'URL_LINK')
                            
                                display_url = f"📄 [{path}]({url_link})"
                                st.sidebar.markdown(display_url)
            
            st.session_state.messages.append({"role": "assistant", "content": response})
    except Exception as e:
        st.error(f"An error occurred in main: {e}")
        
if __name__ == "__main__":
    main()