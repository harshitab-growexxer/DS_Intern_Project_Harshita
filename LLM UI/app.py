import configparser
import streamlit as st
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_groq import ChatGroq
import psycopg2

# Function to read configuration from db_config.ini file
def read_config(filename='config.ini'):
    config = configparser.ConfigParser()
    config.read(filename)
    return config['database'], config['api']

# Function to establish connection with PostgreSQL database using psycopg2
def connect_to_db(config):
    try:
        conn = psycopg2.connect(
            host=config['hostname'],
            port=config['port'],
            user=config['username'],
            password=config['password'],
            database=config['database_name']
        )
        return conn
    except psycopg2.Error as e:
        st.error(f"Error connecting to the database: {e}")
        return None

# Function to fetch table and column information from the database schema
def get_db_schema(conn, table_name):
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name='{table_name}'
        """)
        schema_info = [f'"{row[0]}"' for row in cursor.fetchall()]  # Quote column names
        cursor.close()
        return schema_info
    except psycopg2.Error as e:
        st.error(f"Error fetching database schema: {e}")
        return None

# Function to create SQL query generation chain
def create_sql_chain(conn, target_table, question, api_key):
    schema_info = get_db_schema(conn, target_table)
    if not schema_info:
        return None

    template = f"""
        Based on the table schema of table '{target_table}', write a SQL query to answer the question.
        Only provide the SQL query, without any additional text or characters. The SQL query should contain the exact column names and table name.


        Table schema: {schema_info}
        Question: {question}

        SQL Query:
    """
    prompt = ChatPromptTemplate.from_template(template=template)
    llm = ChatGroq(model="llama3-8b-8192", temperature=0.2, groq_api_key=api_key)

    return (
        RunnablePassthrough(assignments={"schema": schema_info, "question": question})
        | prompt
        | llm
        | StrOutputParser()
    )

# Function to execute SQL query on the database and fetch results
def execute_sql_query(conn, sql_query):
    try:
        cursor = conn.cursor()
        cursor.execute(sql_query)
        results = cursor.fetchall()
        cursor.close()
        return results
    except psycopg2.Error as e:
        st.error(f"Error executing SQL query: {e}")
        return None

# Function to create natural language response based on SQL query results
def create_nlp_answer(conn, sql_query, results, api_key):
    results_str = "\n".join([str(row) for row in results])

    template = f"""
        Based on the results of the SQL query '{sql_query}', write a natural language response.

        Query Results:
        {results_str}
    """
    prompt = ChatPromptTemplate.from_template(template=template)
    llm = ChatGroq(model="llama3-8b-8192", temperature=0.2, groq_api_key=api_key)

    return (
        RunnablePassthrough(assignments={"sql_query": sql_query, "results": results_str})
        | prompt
        | llm
        | StrOutputParser()
    )

def main():
    st.title("Database Query Interface")

    db_config, api_config = read_config('config.ini')

    target_table = 'turbotable'

    conn = connect_to_db(db_config)
    if not conn:
        return

    st.success("Connected to the database successfully!")

    user_query = st.text_input(f"Ask your database a question about {target_table}:")

    if user_query:
        sql_chain = create_sql_chain(conn, target_table, user_query, api_config['groq_api_key'])
        if not sql_chain:
            return

        sql_query_response = sql_chain.invoke({})
        sql_query = sql_query_response.strip()

        st.write(f"Generated SQL Query:\n{sql_query}")

        results = execute_sql_query(conn, sql_query)
        if results:
            st.write("Query Results:")
            st.write(results)

            # Generate natural language response
            nlp_chain = create_nlp_answer(conn, sql_query, results, api_config['groq_api_key'])
            nlp_response = nlp_chain.invoke({})
            st.write(f"Natural Language Response:\n{nlp_response}")
        else:
            st.write("No results found or error occurred.")

    if conn:
        conn.close()

if __name__ == "__main__":
    main()
