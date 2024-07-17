import streamlit as st
import configparser
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_groq import ChatGroq
import psycopg2

# Function to read configuration from config.ini file
def read_config(filename='config.ini'):
    config = configparser.ConfigParser()
    config.read(filename)
    return config['database'], config['api']

# Function to establish connection with PostgreSQL database using psycopg2
def connect_to_db(config):
    conn = psycopg2.connect(
        host=config['hostname'],
        port=config['port'],
        user=config['username'],
        password=config['password'],
        database=config['database_name']
    )
    return conn

# Function to fetch table and column information from the database schema
def get_db_schema(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name='{table_name}'
    """)
    schema_info = [f'"{row[0]}"' for row in cursor.fetchall()]  # Quote column names
    cursor.close()
    return schema_info

# Function to create SQL query generation chain
def create_sql_chain(conn, target_table, question, api_key):
    schema_info = get_db_schema(conn, target_table)

    template = f"""
        Based on the table schema of table '{target_table}', write a SQL query to answer the question.
        ONLY PROVIDE THE SQL QUERY, WITHOUT ANY ADDITIONAL TEXT OR CHARACTERS. The SQL query should contain the exact COLUMN NAMES and TABLE name.
        Use proper statistical functions and mathematical functions which are supported by PostgreSQL.
        Also consider the datatypes of columns.

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
    cursor = conn.cursor()
    try:
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

# Streamlit app
def main():
    st.title("Database Query and NLP Response")

    db_config, api_config = read_config('config.ini')

    target_table = 'turbofan_with_rul'

    try:
        conn = connect_to_db(db_config)
        st.success("Connected to the database successfully!")

        user_query = st.text_input("Ask your database a question about " + target_table + ": ")

        if st.button("Get Response"):
            try:
                # Generate SQL query
                sql_chain = create_sql_chain(conn, target_table, user_query, api_config['groq_api_key'])
                sql_query_response = sql_chain.invoke({})
                sql_query = sql_query_response.strip()
                st.write(f"Generated SQL Query:\n{sql_query}")

                # Execute SQL query
                results = execute_sql_query(conn, sql_query)
                if results:
                    st.write("Query Results:")
                    for row in results:
                        st.write(row)

                    # Generate natural language response
                    nlp_chain = create_nlp_answer(conn, sql_query, results, api_config['groq_api_key'])
                    nlp_response = nlp_chain.invoke({})
                    st.write(f"Natural Language Response:\n{nlp_response}")
                else:
                    st.warning("No results found or error occurred.")
            except Exception as err:
                st.error(f"Error querying the database or generating NLP response: {err}")
    except Exception as err:
        st.error(f"Error connecting to the database: {err}")
    finally:
        if 'conn' in locals() and conn is not None:
            conn.close()

if __name__ == "__main__":
    main()


















































# import streamlit as st
# import configparser
# from langchain_core.runnables import RunnablePassthrough
# from langchain_core.output_parsers import StrOutputParser
# from langchain_core.prompts import ChatPromptTemplate
# from langchain_groq import ChatGroq
# import psycopg2

# # Function to read configuration from config.ini file
# def read_config(filename='config.ini'):
#     config = configparser.ConfigParser()
#     config.read(filename)
#     return config['database'], config['api']

# # Function to establish connection with PostgreSQL database using psycopg2
# def connect_to_db(config):
#     conn = psycopg2.connect(
#         host=config['hostname'],
#         port=config['port'],
#         user=config['username'],
#         password=config['password'],
#         database=config['database_name']
#     )
#     return conn

# # Function to fetch table and column information from the database schema
# def get_db_schema(conn, table_name):
#     cursor = conn.cursor()
#     cursor.execute(f"""
#         SELECT column_name
#         FROM information_schema.columns
#         WHERE table_schema='public' AND table_name='{table_name}'
#     """)
#     schema_info = [f'"{row[0]}"' for row in cursor.fetchall()]  # Quote column names
#     cursor.close()
#     return schema_info

# # Function to create SQL query generation chain
# def create_sql_chain(conn, target_table, question, api_key):
#     schema_info = get_db_schema(conn, target_table)

#     template = f"""
#         Based on the table schema of table '{target_table}', write a SQL query to answer the question.
#         ONLY PROVIDE THE SQL QUERY, WITHOUT ANY ADDITIONAL TEXT OR CHARACTERS. The SQL query should contain the exact COLUMN NAMES and TABLE name.
#         Use proper statistical functions and mathematical functions which are supported by PostgreSQL.
#         Also consider the datatypes of columns.

#         Table schema: {schema_info}
#         Question: {question}

#         SQL Query:
#     """
#     prompt = ChatPromptTemplate.from_template(template=template)
#     llm = ChatGroq(model="llama3-8b-8192", temperature=0.2, groq_api_key=api_key)

#     return (
#         RunnablePassthrough(assignments={"schema": schema_info, "question": question})
#         | prompt
#         | llm
#         | StrOutputParser()
#     )

# # Function to execute SQL query on the database and fetch results
# def execute_sql_query(conn, sql_query):
#     cursor = conn.cursor()
#     try:
#         cursor.execute(sql_query)
#         results = cursor.fetchall()
#         cursor.close()
#         return results
#     except psycopg2.Error as e:
#         st.error(f"Error executing SQL query: {e}")
#         return None

# # Function to create natural language response based on SQL query results
# def create_nlp_answer(conn, sql_query, results, api_key):
#     results_str = "\n".join([str(row) for row in results])

#     template = f"""
#         Based on the results of the SQL query '{sql_query}', write a natural language response.

#         Query Results:
#         {results_str}
#     """
#     prompt = ChatPromptTemplate.from_template(template=template)
#     llm = ChatGroq(model="llama3-8b-8192", temperature=0.2, groq_api_key=api_key)

#     return (
#         RunnablePassthrough(assignments={"sql_query": sql_query, "results": results_str})
#         | prompt
#         | llm
#         | StrOutputParser()
#     )

# # Streamlit app
# def main():
#     st.title("Database Query and NLP Response")

#     db_config, api_config = read_config('config.ini')

#     target_table = 'turbotable'

#     try:
#         conn = connect_to_db(db_config)
#         st.success("Connected to the database successfully!")

#         user_query = st.text_input("Ask your database a question about " + target_table + ": ")

#         if st.button("Generate SQL Query"):
#             try:
#                 sql_chain = create_sql_chain(conn, target_table, user_query, api_config['groq_api_key'])
#                 sql_query_response = sql_chain.invoke({})
#                 sql_query = sql_query_response.strip()
#                 st.write(f"Generated SQL Query:\n{sql_query}")
#             except Exception as err:
#                 st.error(f"Error generating SQL query: {err}")

#         if st.button("Show Explanation and Results"):
#             try:
#                 sql_chain = create_sql_chain(conn, target_table, user_query, api_config['groq_api_key'])
#                 sql_query_response = sql_chain.invoke({})
#                 sql_query = sql_query_response.strip()
#                 st.write(f"Generated SQL Query:\n{sql_query}")

#                 results = execute_sql_query(conn, sql_query)
#                 if results:
#                     st.write("Query Results:")
#                     for row in results:
#                         st.write(row)

#                     # Generate natural language response
#                     nlp_chain = create_nlp_answer(conn, sql_query, results, api_config['groq_api_key'])
#                     nlp_response = nlp_chain.invoke({})
#                     st.write(f"Natural Language Response:\n{nlp_response}")
#                 else:
#                     st.warning("No results found or error occurred.")
#             except Exception as err:
#                 st.error(f"Error querying the database or generating NLP response: {err}")
#     except Exception as err:
#         st.error(f"Error connecting to the database: {err}")
#     finally:
#         if 'conn' in locals() and conn is not None:
#             conn.close()

# if __name__ == "__main__":
#     main()





















































# # import streamlit as st
# # import configparser
# # from langchain_core.runnables import RunnablePassthrough
# # from langchain_core.output_parsers import StrOutputParser
# # from langchain_core.prompts import ChatPromptTemplate
# # from langchain_groq import ChatGroq
# # import psycopg2

# # # Function to read configuration from config.ini file
# # def read_config(filename='config.ini'):
# #     config = configparser.ConfigParser()
# #     config.read(filename)
# #     return config['database'], config['api']

# # # Function to establish connection with PostgreSQL database using psycopg2
# # def connect_to_db(config):
# #     conn = psycopg2.connect(
# #         host=config['hostname'],
# #         port=config['port'],
# #         user=config['username'],
# #         password=config['password'],
# #         database=config['database_name']
# #     )
# #     return conn

# # # Function to fetch table and column information from the database schema
# # def get_db_schema(conn, table_name):
# #     cursor = conn.cursor()
# #     cursor.execute(f"""
# #         SELECT column_name
# #         FROM information_schema.columns
# #         WHERE table_schema='public' AND table_name='{table_name}'
# #     """)
# #     schema_info = [f'"{row[0]}"' for row in cursor.fetchall()]  # Quote column names
# #     cursor.close()
# #     return schema_info

# # # Function to create SQL query generation chain
# # def create_sql_chain(conn, target_table, question, api_key):
# #     schema_info = get_db_schema(conn, target_table)

# #     template = f"""
# #         Based on the table schema of table '{target_table}', write a SQL query to answer the question.
# #         Only provide the SQL query, without any additional text or characters. The SQL query should contain the exact column names and table name.

# #         Table schema: {schema_info}
# #         Question: {question}

# #         SQL Query:
# #     """
# #     prompt = ChatPromptTemplate.from_template(template=template)
# #     llm = ChatGroq(model="llama3-8b-8192", temperature=0.2, groq_api_key=api_key)

# #     return (
# #         RunnablePassthrough(assignments={"schema": schema_info, "question": question})
# #         | prompt
# #         | llm
# #         | StrOutputParser()
# #     )

# # # Function to execute SQL query on the database and fetch results
# # def execute_sql_query(conn, sql_query):
# #     cursor = conn.cursor()
# #     try:
# #         cursor.execute(sql_query)
# #         results = cursor.fetchall()
# #         cursor.close()
# #         return results
# #     except psycopg2.Error as e:
# #         st.error(f"Error executing SQL query: {e}")
# #         return None

# # # Function to create natural language response based on SQL query results
# # def create_nlp_answer(conn, sql_query, results, api_key):
# #     results_str = "\n".join([str(row) for row in results])

# #     template = f"""
# #         Based on the results of the SQL query '{sql_query}', write a natural language response.

# #         Query Results:
# #         {results_str}
# #     """
# #     prompt = ChatPromptTemplate.from_template(template=template)
# #     llm = ChatGroq(model="llama3-8b-8192", temperature=0.2, groq_api_key=api_key)

# #     return (
# #         RunnablePassthrough(assignments={"sql_query": sql_query, "results": results_str})
# #         | prompt
# #         | llm
# #         | StrOutputParser()
# #     )

# # # Streamlit app
# # def main():
# #     st.title("Database Query and NLP Response")

# #     db_config, api_config = read_config('config.ini')

# #     target_table = 'turbotable'

# #     conn = None

# #     try:
# #         conn = connect_to_db(db_config)
# #         st.success("Connected to the database successfully!")

# #         user_query = st.text_input("Ask your database a question about " + target_table + ": ")

# #         if st.button("Submit"):
# #             sql_chain = create_sql_chain(conn, target_table, user_query, api_config['groq_api_key'])
# #             sql_query_response = sql_chain.invoke({})
# #             sql_query = sql_query_response.strip()

# #             st.write(f"Generated SQL Query:\n{sql_query}")

# #             results = execute_sql_query(conn, sql_query)
# #             if results:
# #                 st.write("Query Results:")
# #                 for row in results:
# #                     st.write(row)

# #                 # Generate natural language response
# #                 nlp_chain = create_nlp_answer(conn, sql_query, results, api_config['groq_api_key'])
# #                 nlp_response = nlp_chain.invoke({})
# #                 st.write(f"Natural Language Response:\n{nlp_response}")
# #             else:
# #                 st.warning("No results found or error occurred.")

# #     except Exception as err:
# #         st.error(f"Error connecting to or querying the database: {err}")

# #     finally:
# #         if conn is not None:
# #             conn.close()

# # if __name__ == "__main__":
# #     main()
