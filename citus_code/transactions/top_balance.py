import psycopg2
from psycopg2 import sql

def top_balance(host, database, user, password):
    conn = None
    try:
        # Establish connection to the database
        conn = psycopg2.connect(host=host, database=database, user=user, password=password)
        cur = conn.cursor()
        print('connection success to:')
        print(f'host: {host}  database:{database}  user:{user}')

        # Begin the transaction
        conn.autocommit = False

        # find top-10 customers ranked in descending balance order
        top_customer_query = sql.SQL("""
            select c_first, c_middle, c_last, c_balance, w_name, d_name 
            from customer
            left join warehouse on c_w_id = w_id 
            left join district on c_w_id = d_w_id and c_d_id = d_id 
            order by c_balance desc 
            limit 10 
        """)
        cur.execute(top_customer_query)
        top_customers_info = cur.fetchall()

        i = 0 
        if top_customers_info:
            for customer_info in top_customers_info:
                i+=1
                c_first, c_middle, c_last, c_balance, w_name, d_name = customer_info
                print(f"***********************************************")
                print(f"Customer No.{i}:")
                print(f"Customer Name: {c_first}-{c_middle}-{c_last}")
                print(f"Outstanding Balance: {c_balance}")
                print(f"Warehouse Name: {w_name}")
                print(f"District Name: {d_name}")
        else:
            print(f"find no customer!!")

        # Commit the transaction
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

# Example usage
top_balance(host="localhost", database="project", user="cs4224d", password="1234")
