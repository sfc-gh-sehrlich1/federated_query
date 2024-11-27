#%%
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

import streamlit as st
import helper_function as hf

import config as cfg
import time

print(st.session_state)
if 'session_code' not in st.session_state:
    connection_parameters = cfg.snowflake_conn_prop
    session = Session.builder.configs(connection_parameters).create()
else:
    connection_parameters = cfg.snowflake_conn_prop
    session = Session.builder.configs(connection_parameters).create()
    session.use_database("FEDERATED_CONNNECTION_DB")
    session.use_schema("PUBLIC")



def create_procedure(_session : Session ,jdbc_url, username, password, connection_source):

    connection_class = connection_source_class.get(connection_source)[1]
    stored_proc = hf.create_procedure_sf(connection_source, connection_class, username,password, jdbc_url, st.session_state.network_integration_name)
    print(stored_proc)
    _session.sql(stored_proc).collect() 


def create_connection_location(_session : Session):
    st.session_state.session_code = True
    create_database_sql = f"""CREATE DATABASE IF NOT EXISTS FEDERATED_CONNNECTION_DB;"""
    _session.sql(create_database_sql).collect()
    _session.use_database("FEDERATED_CONNNECTION_DB")
    _session.use_schema("PUBLIC")




def load_driver(_session : Session ,host, connection_source):

    load_driver_sql = f"""CREATE STAGE IF NOT EXISTS connection_stage;"""
    _session.sql(load_driver_sql).collect()
    _session.file.put(f"../pythoncode/drivers/{connection_source}.jar", "@connection_stage/driver",auto_compress=False)

    print("Driver Loaded Successfully")

def test_connection(_session : Session, jdbc_url, username, password, connection_source):

    connection_class = connection_source_class.get(connection_source)[1]
    print(connection_class)
    connection_sproc = hf.create_test_connection_sproc(connection_source, connection_class, st.session_state.network_integration_name)
    print(connection_sproc)
    _session.sql(connection_sproc).collect()

    connection_sql_call = f"""call test_connection('{jdbc_url}','{username}','{password}');""" 

    df = _session.sql(connection_sql_call)
    df.collect()
    connection_result = df.to_pandas()["TEST_CONNECTION"][0]
    print(connection_result)

    if connection_result == "Success":
        return True
    else:
        return connection_result




def create_network_rule(_session : Session ,host, port):
        # Prepare the SQL command to create the network policy
    
    network_rule_name = f"snowflake_network_rule"

    create_rule_sql = f"""
    CREATE OR REPLACE NETWORK RULE {network_rule_name}
    MODE = EGRESS

    TYPE = HOST_PORT

    VALUE_LIST = ('{host}:{port}')
    """

    print(create_rule_sql)
    _session.sql(create_rule_sql).collect()
    # session.sql(create_policy_sql).collect()
    # Execute the query using the session object from Snowpark
    # session.sql(create_policy_sql).collect()
    return network_rule_name 


def create_network_integration(_session, network_policy_name, connection_source):
    network_integration_name = f"snowflake_network_integration_{connection_source}"

    create_network_integration_sql = f"""CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION {network_integration_name}
    ALLOWED_NETWORK_RULES = ('{network_policy_name}')
    ENABLED = true
    """
    

    print(create_network_integration_sql)
    _session.sql(create_network_integration_sql).collect()

    return network_integration_name


#%%

# Set the title for your Streamlit app
st.title('Create Procedure to call databases')

procedure_name = st.text_input('Enter Name of Procedure')

# Dropdown 1
connection_source_options = ['postgres', 'mysql', 'sqlserver', 'sap']

connection_source_class = {'postgres': ['postgresql', 'org.postgresql.Driver'], \
                           'mysql': ['mysql', 'com.mysql.jdbc.Driver'] , \
                           'sqlserver':['sqlserver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver'], \
                           'redshift': ['redshift','com.amazon.redshift.jdbc.Driver'], \
                           'sap': 'com.sap.db.jdbc.Driver'}
connection_source = st.selectbox('Select from Dropdown 1', connection_source_options)

# Dropdown 2

st.write("connection details")
host = st.text_input('Enter host (e.g. 8.8.8.8 or ecw-ecw-ecw-ecw.ec2.internal)' , value='172.168.72.1')
port = st.text_input('Enter port')
database_name = st.text_input('Enter database name')

st.write("credentials")
username = st.text_input('Enter username')
password = st.text_input('Enter password', type="password")

st.write("url otions, e.g. encrypt=false;trustServerCertificate=true;loginTimeout=30")

options = st.text_input('Enter Options')

if options:
    jdbc_url = f"jdbc:{connection_source_class.get(connection_source)[0]}://{host}:{port}/{database_name};{options}"
else:
    jdbc_url = f"jdbc:{connection_source_class.get(connection_source)[0]}://{host}:{port}/{database_name}"

st.write(jdbc_url)

col1, col2, col3, col4 = st.columns(4)

with col1: 
    network_rule_button = st.button('Create Network Rule')
    

with col2:
    network_integration_button = st.button('Create Network Integration')


with col3:
    connection_button = st.button('Test Connection')

with col4:
    procedure = st.button('Create Procedure')

if network_rule_button:
    create_connection_location(session)
    network_rule_name = create_network_rule(session, host, port)
    st.session_state.network_rule_name = network_rule_name 
    st.write( f"Network policy '{network_rule_name}' created successfully.")
    load_driver(session, host, connection_source)
    st.write( f"Driver loaded successfully.")

if network_integration_button:
    print(st.session_state.network_rule_name)
    if  st.session_state.network_rule_name is None:
        st.error("Network policy must be created first!")
    else:
        print(st.session_state.network_rule_name)
        network_integration_name = create_network_integration(session, st.session_state.network_rule_name, connection_source)
        st.session_state.network_integration_name = network_integration_name 
        st.write( f"Network Integration '{network_integration_name}' created successfully.")

if connection_button:
    if 'network_integration_name' not in st.session_state:
        st.error("Network Integration must be created first!")
    else:
        if test_connection(session, jdbc_url, username, password, connection_source) == True: 
            st.write( f"Connection to '{jdbc_url}' created successfully.")
            st.session_state.successful_connection = True 

        else:
            st.write( f"Connection to '{jdbc_url}' failed.  Please verify component details and try again.")

if procedure:
    if 'successful_connection' not in st.session_state:
        st.error("Make sure connection is succssful first!")
    else:
        create_procedure(session, jdbc_url, username, password, connection_source)
        st.write( f"Procedure created successfully.")