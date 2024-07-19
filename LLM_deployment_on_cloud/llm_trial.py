import streamlit as st
import configparser
import psycopg2
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_groq import ChatGroq

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

    - **engine**: The specific engine number.
    - **cycles**: The number of flight cycles completed by the engine.
    - **altitude**: The height above sea level in feet (ft).
    - **mach_number**: The ratio of the speed of the aircraft to the speed of sound, dimensionless (-).
    - **throttle_resolver_angle**: The position of the throttle lever, represented as a percentage (%).
    - **fan_inlet_temp**: Total temperature at fan inlet. Measures the temperature of the air entering the fan section of the engine. Units: °R (degrees Rankine).
    - **lpc_outlet_temp**: Total temperature at LPC outlet. Measures the temperature of the air at the outlet of the Low-Pressure Compressor (LPC). Units: °R.
    - **hpc_outlet_temp**: Total temperature at HPC outlet. Measures the temperature of the air at the outlet of the High-Pressure Compressor (HPC). Units: °R.
    - **lpt_outlet_temp**: Total temperature at LPT outlet. Measures the temperature of the air at the outlet of the Low-Pressure Turbine (LPT). Units: °R.
    - **fan_inlet_pressure**: Pressure at fan inlet. Measures the static pressure of the air entering the fan section of the engine. Units: psia (pounds per square inch absolute).
    - **bypass_duct_pressure**: Total pressure in bypass-duct. Measures the total pressure in the bypass-duct of the engine. Units: psia.
    - **hpc_outlet_pressure**: Total pressure at HPC outlet. Measures the total pressure at the outlet of the High-Pressure Compressor (HPC). Units: psia.
    - **fan_speed**: Physical fan speed. Measures the rotational speed of the fan section of the engine. Units: rpm (revolutions per minute).
    - **core_speed**: Physical core speed. Measures the rotational speed of the core (combustor and turbine sections) of the engine. Units: rpm.
    - **engine_pressure_ratio**: Engine pressure ratio (P50/P2). Represents the ratio of the total pressure at the HPC outlet (P30) to the pressure at the fan inlet (P2). Units: -- (dimensionless).
    - **hpc_outlet_static_pressure**: Static pressure at HPC outlet. Measures the static pressure at the outlet of the High-Pressure Compressor (HPC). Units: psia.
    - **fuel_ps30_ratio**: Ratio of fuel flow to Ps30. Represents the ratio of fuel flow to the static pressure at the HPC outlet (Ps30). Units: pps/psi (pounds per second per pound per square inch).
    - **fan_speed_ratio**: Corrected fan speed. Measures the fan speed corrected for ambient conditions. Units: rpm.
    - **core_speed_ratio**: Corrected core speed. Measures the core speed corrected for ambient conditions. Units: rpm.
    - **bypass_ratio**: Bypass Ratio. Indicates the ratio of the mass flow rate of air through the bypass duct to the mass flow rate through the engine core. Units: -- (dimensionless).
    - **burner_fuel_air_ratio**: Burner fuel-air ratio. Represents the ratio of fuel flow to the airflow entering the burner. Units: -- (dimensionless).
    - **bleed_enthalpy**: Bleed Enthalpy. Measures the enthalpy (heat content) of the air bled from the engine for various purposes such as cooling. Units: -- (dimensionless).
    - **demanded_fan_speed**: Demanded fan speed. Represents the demanded or desired rotational speed of the fan section of the engine. Units: rpm.
    - **demanded_corrected_fan_speed**: Demanded corrected fan speed. Represents the demanded or desired corrected rotational speed of the fan section of the engine. Units: rpm.
    - **hpt_coolant_bleed**: HPT coolant bleed. Measures the mass flow rate of coolant bleed from the High-Pressure Turbine (HPT). Units: lbm/s (pounds per second).
    - **lpt_coolant_bleed**: LPT coolant bleed. Measures the mass flow rate of coolant bleed from the Low-Pressure Turbine (LPT). Units: lbm/s.
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

