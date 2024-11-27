
#%%
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

def create_test_connection_sproc(connection_source, connection_class, network_integration_name):
    sql_statement  = f"""CREATE OR REPLACE procedure test_connection(connection_string string, username string, password string) 
    RETURNS String
    LANGUAGE JAVA
    RUNTIME_VERSION = '11'
    IMPORTS = ('@FEDERATED_CONNNECTION_DB.PUBLIC.CONNECTION_STAGE/driver/{connection_source}.jar')
    packages = ('com.snowflake:snowpark:latest')
    EXTERNAL_ACCESS_INTEGRATIONS = ({network_integration_name})
    //SECRETS = ('cred' = external_database_cred )
    HANDLER = 'test_connection.process'
    AS $$

    import java.sql.*;
    import java.util.*;
    import com.snowflake.snowpark_java.Row;
    import com.snowflake.snowpark_java.types.StructField;
    import com.snowflake.snowpark_java.types.StructType;
    import com.snowflake.snowpark_java.types.DataTypes;
    import com.snowflake.snowpark_java.DataFrame;
    import com.snowflake.snowpark_java.Session;
    import com.snowflake.snowpark_java.SaveMode;


    public class test_connection {{
        public String process(Session session, String connection_string, String username, String password) {{


            String jdbcUrl = connection_string;


                try {{
                Class.forName("{connection_class}");
                }} catch (ClassNotFoundException e) {{
                    return e.toString();
                }}

                try {{
                Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("select 1");
                

                }} catch (SQLException e) {{
                    e.printStackTrace();
                    return e.toString();

                }}

                return "Success";

        }}
    }}
    $$;"""

    return sql_statement

def create_procedure_sf(connection_source, connection_class, username, password, jdbc_url, network_integration_name):
  sql_statement = f"""CREATE OR REPLACE procedure load_to_table(query_text STRING, table_name string, batch int) 
  RETURNS string
  LANGUAGE JAVA
  RUNTIME_VERSION = '11'
  IMPORTS = ('@FEDERATED_CONNNECTION_DB.PUBLIC.CONNECTION_STAGE/driver/{connection_source}.jar')
  packages = ('com.snowflake:snowpark:latest','com.snowflake:telemetry:latest')
  EXTERNAL_ACCESS_INTEGRATIONS = ({network_integration_name})
   //SECRETS = ('cred' = external_database_cred )
  HANDLER = 'load_to_table.process'
AS $$

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.types.StructField;
import com.snowflake.snowpark_java.types.StructType;
import com.snowflake.snowpark_java.types.DataTypes;
import com.snowflake.snowpark_java.DataFrame;
import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class load_to_table {{

    public static Logger logger = LoggerFactory.getLogger(load_to_table.class);


    public static StructField getColumnTypeName(String columnName, int columndataType) {{
        switch (columndataType) {{
            case Types.VARCHAR: return new StructField(columnName, DataTypes.StringType);
            case Types.INTEGER: return new StructField(columnName, DataTypes.IntegerType);
            // case Types.FLOAT: return "FLOAT";
            case Types.FLOAT: return new StructField(columnName, DataTypes.FloatType);
            case Types.DOUBLE:
                return new StructField(columnName, DataTypes.DoubleType );
            case Types.DATE:
                return new StructField(columnName, DataTypes.DateType );
            case Types.TIMESTAMP:
                return new StructField(columnName, DataTypes.TimestampType );
            case Types.BOOLEAN:
                return new StructField(columnName, DataTypes.BooleanType);
            case Types.DECIMAL:
                return new StructField(columnName, DataTypes.createDecimalType(10, 2));
            case Types.BIGINT:
                return new StructField(columnName, DataTypes.LongType);
            case Types.SMALLINT:
                return new StructField(columnName, DataTypes.ShortType);
            case Types.TINYINT:
                return new StructField(columnName, DataTypes.ByteType);
            case Types.CHAR:
                return new StructField(columnName, DataTypes.StringType);
            case Types.BINARY:
                return new StructField(columnName, DataTypes.BinaryType);
            case Types.BIT:
                return new StructField(columnName, DataTypes.BooleanType);
            case Types.NUMERIC:
                return new StructField(columnName, DataTypes.createDecimalType(10, 2));
            case Types.TIME:
                return new StructField(columnName, DataTypes.TimestampType);
            case Types.NULL:
                return new StructField(columnName, DataTypes.StringType);
            default:
                return new StructField(columnName, DataTypes.StringType);
        }}
    }}


    public String process(Session session,String query_text, String table_name, Long BATCH_LIMIT) {{

    logger.info("process started");
    session.sql("DROP TABLE IF EXISTS " + table_name).collect();
    String jdbcUrl = "{jdbc_url}";
    String username = "{username}";
    String password = "{password}";
    String sql_query_text = query_text;

    // Create a connection to the database

    try {{
        Class.forName("{connection_class}");
    }} catch (ClassNotFoundException e) {{
        e.printStackTrace();
        return Integer.toString(2);
    }}

    try {{

    Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
    Statement statement = connection.createStatement();
    logger.info("result set started");
    ResultSet resultSet = statement.executeQuery(sql_query_text);
    logger.info("result set loaded");



    ResultSetMetaData metaData = resultSet.getMetaData();
    int columnCount = metaData.getColumnCount();

    // Define the schema for Snowpark's DataFrame based on the ResultSet metadata
    List<StructField> fields = new ArrayList<>();
    for (int i = 1; i <= columnCount; i++) {{
        int columndataType = metaData.getColumnType(i);
        String columnName = metaData.getColumnName(i);
        StructField field = getColumnTypeName(columnName, columndataType);
        fields.add(field);  // Use new StructField() instead of create()
    }}
    StructType schema = new StructType(fields.toArray(new StructField[0]));

    ExecutorService executor = Executors.newFixedThreadPool(8);
    List<Future<Void>> tasks = new ArrayList<>();

    List<Row> rows = new ArrayList<>();
    DataFrame df = null;
    boolean firstBatch = true;
    while (resultSet.next()) {{
        Object[] values = new Object[columnCount];
        for (int i = 1; i <= columnCount; i++) {{
            values[i - 1] = resultSet.getObject(i);
        }}
        rows.add(Row.create(values));

        if (rows.size() >= BATCH_LIMIT && firstBatch) {{
                df = session.createDataFrame(rows.toArray(new Row[0]),schema);
                df.write().mode(SaveMode.Overwrite).saveAsTable(table_name);
                logger.info("first bactch loaded");
                firstBatch=false;
                rows.clear();
            }}
        else if (rows.size()>=BATCH_LIMIT && !firstBatch  && !resultSet.isLast()){{
                DataFrame dfp = session.createDataFrame(rows.toArray(new Row[0]),schema);
                dfp.write().mode(SaveMode.Append).saveAsTable(table_name);
                logger.info("append bactch loaded");
                rows.clear();
            
            }}
        else if (resultSet.isLast()) {{
                DataFrame dfz = session.createDataFrame(rows.toArray(new Row[0]),schema);
                dfz.write().mode(SaveMode.Append).saveAsTable(table_name);
                break;
            }}
        }}


    }} catch (SQLException e) {{
        e.printStackTrace();
        return e.toString();
    }}

    return Integer.toString(1);

    }}
}}
$$;"""
  
  return sql_statement