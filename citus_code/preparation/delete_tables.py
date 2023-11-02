import psycopg2
from psycopg2 import sql

# parameters link to citus 
host="localhost"
database="project"
user="cs4224d"
password="1234"

# connect to citus
conn = psycopg2.connect(host=host, database=database, user=user, password=password)
cur = conn.cursor()
# print('connection success to:')
# print(f'host: {host}  database:{database}  user:{user}')

try: 
    data_modeling = sql.SQL("""
        DO $$
        DECLARE 
            r RECORD;
        BEGIN
            FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public')
            LOOP
                EXECUTE 'DROP TABLE IF EXISTS ' || r.tablename || ' CASCADE';
            END LOOP;
        END $$;
    """)

    cur.execute(data_modeling)
    conn.commit()

except psycopg2.DatabaseError as e:
    # If an error occurs, rollback the transaction
    conn.rollback()
    print(f"Error: {e}")

finally:
    # Close the cursor and connection
    if conn:
        conn.close()

    if cur: 
        cur.close()