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
    schema_info = [f'{row[0]}' for row in cursor.fetchall()]  # Quote column names
    cursor.close()
    return schema_info

def create_sql_chain(conn, target_table, question, api_key):
    schema_info = get_db_schema(conn, target_table)

    template = f"""
        Based on the table schema of table '{target_table}', write a SQL query to answer the question.

        Ensure the following:
        - Only provide the SQL query without any additional text or characters.
        - Use only PostgreSQL-supported functions.
        - Use proper statistical and mathematical functions for the query.
        - Include the table name in the FROM clause.
        - Use the following columns in the database: {schema_info}
        - If identifying outliers, consider using percentile functions and ensure the query is structured correctly to avoid SQL syntax errors.
        - Ensure the query groups by relevant columns if necessary and avoids SQL syntax errors.
        - For range calculation, use MAX and MIN functions.
        - For standard deviation calculation, use the STDDEV function.
        - For describing a column or asked for trend of a column, provide the 5-number summary (minimum, first quartile, median, third quartile, maximum).
        - When asked to compare values for train and test data, use 'source' column where train data is 0 and test data is 1.
        - When asked if the data has any null values, calculate the count of null values

        For example - 
        Example1 -> how many engines are included in the data?
        sql query : SELECT COUNT(DISTINCT engine) FROM turbofan_engine_with_rul;

        Example2 -> are there any outliers in the data?
        sql query : WITH percentiles AS (
                        SELECT
                            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY "fan_inlet_temp") AS Q1,
                            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY "fan_inlet_temp") AS Q3
                        FROM "turbofan_engine_with_rul"
                    ),
                    iqr AS (
                        SELECT
                            Q1,
                            Q3,
                            Q3 - Q1 AS IQR,
                            Q1 - 1.5 * (Q3 - Q1) AS lower_bound,
                            Q3 + 1.5 * (Q3 - Q1) AS upper_bound
                        FROM percentiles
                    )
                    SELECT
                        *
                    FROM
                        "turbofan_engine_with_rul",
                        iqr
                    WHERE
                        "fan_inlet_temp" < lower_bound OR "fan_inlet_temp" > upper_bound;


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
    except psycopg2.Error:
        return None

# Function to create natural language response based on SQL query results and the original question
def create_nlp_answer(question, sql_query, results, api_key):
    results_str = "\n".join([str(row) for row in results])

    template = f"""
        Based on the question '{question}' and the results of the SQL query '{sql_query}', write a natural language response.

        Instructions:
        - If the query is unrelated to the dataset, politely inform the user to ask data-related questions.
    
        Question: {question}
        Query Results:
        {results_str}
    """
    prompt = ChatPromptTemplate.from_template(template=template)
    llm = ChatGroq(model="llama3-8b-8192", temperature=0.2, groq_api_key=api_key)

    return (
        RunnablePassthrough(assignments={"question": question, "sql_query": sql_query, "results": results_str})
        | prompt
        | llm
        | StrOutputParser()
    )

def main():
    st.title("Turbofan Engine Degradation Simulation")

    # Dataset description
    dataset_description = """
    The dataset, Turbofan Engine Degradation Simulation, has the following columns:

    - **engine_number**: A unique identifier for each data entry.
    - **cycles**: The number of flight cycles completed by the engine.
    - **altitude**: The height above sea level in feet (ft).
    - **mach_number**: The ratio of the speed of the aircraft to the speed of sound, dimensionless (-).
    - **throttle_resolver_angle**: The position of the throttle lever, represented as a percentage (%).
    - **source**: The source or origin of the data.
    - **fan_inlet_temp**: The temperature of the air at the fan inlet, measured in Rankine (°R).
    - **lpc_outlet_temp**: The temperature of the air at the outlet of the Low-Pressure Compressor, measured in Rankine (°R).
    - **hpc_outlet_temp**: The temperature of the air at the outlet of the High-Pressure Compressor, measured in Rankine (°R).
    - **lpt_outlet_temp**: The temperature of the air at the outlet of the Low-Pressure Turbine, measured in Rankine (°R).
    - **fan_inlet_pressure**: The pressure of the air at the fan inlet, measured in pounds per square inch absolute (psia).
    - **bypass_duct_pressure**: The pressure of the air in the bypass duct, measured in pounds per square inch absolute (psia).
    - **fan_speed**: The rotational speed of the fan, measured in revolutions per minute (rpm).
    - **core_speed**: The rotational speed of the core, measured in revolutions per minute (rpm).
    - **hpc_outlet_static_pressure**: The static pressure at the outlet of the High-Pressure Compressor, measured in pounds per square inch absolute (psia).
    - **hpc_outlet_pressure**: The pressure of the air at the outlet of the High-Pressure Compressor, measured in pounds per square inch absolute (psia).
    - **engine_pressure_ratio**: The ratio of the pressure at the outlet of the engine to the pressure at the inlet, dimensionless (-).
    - **fuel_ps30_ratio**: The ratio of the fuel flow rate to the static pressure at the HPC outlet, measured in pounds per second per pound per square inch (pps/psi).
    - **fan_speed_ratio**: The ratio of the fan speed to a reference speed, dimensionless (-).
    - **core_speed_ratio**: The ratio of the core speed to a reference speed, dimensionless (-).
    - **bypass_ratio**: The ratio of the mass flow rate of air bypassing the engine to the mass flow rate of air passing through the engine core, dimensionless (-).
    - **burner_fuel_air_ratio**: The ratio of the fuel flow to the air flow in the bypass duct, dimensionless (-).
    - **hpt_coolant_bleed**: The amount of air bled from the high-pressure turbine for cooling purposes, measured in pounds per second (lbm/s).
    - **demanded_fan_speed**: The desired or demanded speed of the fan, measured in revolutions per minute (rpm).
    - **corrected_fan_speed**: The desired or demanded corrected fan speed, dimensionless (-).
    - **hpt_coolant_bleed_flow_1**: The amount of air bled from the high-pressure turbine for cooling purposes, measured in pounds per second (lbm/s).
    - **hpt_coolant_bleed_flow_2**: Another measure of the amount of air bled from the high-pressure turbine for cooling purposes, measured in pounds per second (lbm/s).
    """
    st.sidebar.markdown(dataset_description)

    db_config, api_config = read_config('config.ini')

    target_table = 'turbofan_engine_with_rul'

    try:
        conn = connect_to_db(db_config)
        st.success("Connected to the database successfully!")

        user_query = st.text_input("Ask a question about the Database " + target_table + ": ")

        if st.button("Get Answer"):
            try:
                # Generate SQL query
                sql_chain = create_sql_chain(conn, target_table, user_query, api_config['groq_api_key'])
                sql_query_response = sql_chain.invoke({})
                sql_query = sql_query_response.strip()

                # Execute SQL query
                results = execute_sql_query(conn, sql_query)
                if results:
                    # Generate natural language response
                    nlp_chain = create_nlp_answer(user_query, sql_query, results, api_config['groq_api_key'])
                    nlp_response = nlp_chain.invoke({})
                    st.write(f"Answer:\n{nlp_response}")
                else:
                    st.warning("No response generated.")
            except Exception:
                st.warning("No response generated.")
    except Exception:
        st.warning("No response generated.")
    finally:
        if 'conn' in locals() and conn is not None:
            conn.close()

if __name__ == "__main__":
    main()
