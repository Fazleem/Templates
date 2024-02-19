from sqlalchemy import create_engine, inspect
import pandas as pd

# Database connection details
server = 'YOUR_SERVER_NAME'
database = 'YOUR_DATABASE_NAME'
username = 'YOUR_USERNAME'
password = 'YOUR_PASSWORD'
driver = 'SQL+Server'

# Create the database engine
engine = create_engine(f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}")

def verify_database_connection(engine):
    try:
        with engine.connect() as conn:
            # If this point is reached, the connection is successful
            print("Database connection verified.")
    except Exception as e:
        print(f"Failed to connect to the database: {e}")
        raise

def check_tables_exist(engine, tables):
    inspector = inspect(engine)
    available_tables = inspector.get_table_names()
    missing_tables = [table for table in tables if table not in available_tables]
    if missing_tables:
        print(f"Missing tables: {missing_tables}")
        raise ValueError("One or more tables are missing from the database.")
    else:
        print("All specified tables exist.")

def check_tables_have_data(engine, tables):
    with engine.connect() as conn:
        for table in tables:
            result = conn.execute(f"SELECT TOP 1 * FROM {table}")
            if result.fetchone() is None:
                print(f"No data found in table: {table}")
                raise ValueError(f"Table {table} is empty.")
            else:
                print(f"Data verified in table: {table}")

def join_and_process_data(engine):
    # Before proceeding, verify the connection and table/data presence
    verify_database_connection(engine)
    required_tables = ['TableC', 'TableB', 'TableD', 'TableE']
    check_tables_exist(engine, required_tables)
    check_tables_have_data(engine, required_tables)
    
    # Assuming verification is successful, proceed with operations
    query_1 = "SELECT C.*, B.* FROM TableC C JOIN TableB B ON C.JoinColumnCB = B.id"
    df1 = pd.read_sql_query(query_1, engine)
    
    query_2 = "SELECT D.*, B.* FROM TableD D JOIN TableB B ON D.JoinColumnDB = B.id"
    df2 = pd.read_sql_query(query_2, engine)
    
    query_3 = "SELECT E.*, D.* FROM TableE E JOIN TableD D ON E.JoinColumnED = D.id"
    df3 = pd.read_sql_query(query_3, engine)
    
    # Example data manipulation with pandas
    # Example merging DataFrames and cleaning data
    # Final DataFrame operations and insertion into new table
    # Placeholder for actual pandas operations
    
    # final_df.to_sql('final_cleaned_table', con=engine, if_exists='replace', index=False)

# Execute the enhanced function with verifications
join_and_process_data(engine)
