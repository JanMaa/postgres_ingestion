import psycopg2
import pandas as pd
from sqlalchemy import create_engine

file_path = "C:\\Users\\User\\Downloads\\MTA_Daily_Ridership\\MTA_Daily_Ridership_.csv"

df = pd.read_csv(file_path)

df['date'] = pd.to_datetime(df['date'], format='%m/%d/%Y')

try:
    
    host="localhost"
    dbname='bronze'
    user='postgres'
    password='password'
    port=5432

    conn = psycopg2.connect(
        host=host,
        dbname=dbname,
        user=user,
        password=password,
        port=port
    )
    # Create SQLAlchemy engine string
    engine_string = f"""postgresql://{user}:{password}@{host}:{port}/{dbname}"""
    
    # Create engine
    engine = create_engine(engine_string)

    # Create cursor object
    cur = conn.cursor()

    # Generate CREATE TABLE statement based on DataFrame schema
    table_name = "mta_daily_ridership"
    create_statement = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"

    # Map pandas dtypes to PostgreSQL types
    dtype_mapping = {
        'int64': 'BIGINT',
        'float64': 'DECIMAL',
        'object': 'VARCHAR(255)',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP'
    }
    
    # Add columns to CREATE statement
    for column in df.columns:
        pg_type = dtype_mapping.get(str(df[column].dtype), 'VARCHAR(255)')
        create_statement += f"{column} {pg_type},\n"
    
    # Remove trailing comma and close parentheses
    create_statement = create_statement.rstrip(',\n') + "\n);"
    
    print("Generated CREATE TABLE statement:")
    print(create_statement)
    
    # Execute CREATE TABLE statement
    cur.execute(create_statement)
    conn.commit()

    # Load data into the table
    df.to_sql(table_name, engine, if_exists='append', index=False)

except Exception as e:
    print(f"An error occurred: {str(e)}")
finally:
    if 'conn' in locals() and conn:
        conn.close()
    if 'engine' in locals() and engine:
        engine.dispose()