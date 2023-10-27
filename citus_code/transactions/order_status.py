import psycopg2
from psycopg2 import sql
import time

def order_status(host, database, user, password, c_w_id, c_d_id, c_id):
    conn = None
    start_time = time.time()
    try:
        # Establish connection to the database
        conn = psycopg2.connect(host=host, database=database, user=user, password=password)
        cur = conn.cursor()
        print('connection success to:')
        print(f'host: {host}  database:{database}  user:{user}')

        # Begin transaction
        conn.autocommit = False
        # Step0: Lock the orders, customer, and order_line tables
        lock_orders_query = sql.SQL("LOCK TABLE orders IN SHARE MODE")
        cur.execute(lock_orders_query)

        lock_customer_query = sql.SQL("LOCK TABLE customer IN SHARE MODE")
        cur.execute(lock_customer_query)

        lock_order_line_query = sql.SQL("LOCK TABLE order_line IN SHARE MODE")
        cur.execute(lock_order_line_query)

        # step1: get customer information with customer identifier
        customer_info_query = sql.SQL("""
            select c_first, c_middle, c_last, c_balance from customer
            where c_w_id = %s and c_d_id = %s and c_id = %s
        """).format(sql.Literal(c_w_id), sql.Literal(c_d_id), sql.Literal(c_id))
        cur.execute(customer_info_query,(c_w_id,c_d_id,c_id))
        customer_info = cur.fetchone()
       
        if customer_info is not None:
            c_first_name, c_middle_name, c_last_name, c_balance = customer_info

            #step2: query the last order info from orders table 
            last_order_query = sql.SQL("""
                SELECT o_id, o_entry_d, o_carrier_id FROM orders
                WHERE o_w_id = %s AND o_d_id = %s AND o_c_id = %s
                ORDER BY o_id DESC LIMIT 1
            """).format(sql.Literal(c_w_id), sql.Literal(c_d_id), sql.Literal(c_id))
            cur.execute(last_order_query, (c_w_id, c_d_id, c_id))
            last_order_info = cur.fetchone()

            if last_order_info is not None:
                o_id, o_entry_d, o_carrier_id = last_order_info

                #step3: query each item in the last order from order-line table
                order_line_query = sql.SQL("""
                    SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line
                    WHERE ol_w_id = %s AND ol_d_id = %s AND ol_o_id = %s
                """).format(sql.Literal(c_w_id), sql.Literal(c_d_id), sql.Literal(c_id))
                cur.execute(order_line_query, (c_w_id, c_d_id, o_id))
                order_lines = cur.fetchall()

                #step4: print customer info
                print(f"Customer Name: {c_first_name}-{c_middle_name}-{c_last_name}")
                print(f"Customer Balance: {c_balance}")
                print(f"Last Order info:")
                print(f"Last Order ID: {o_id}")
                print(f"Entry Date and Time: {o_entry_d}")
                print(f"Carrier ID: {o_carrier_id}")
                print("Items info in Last Order:")
                for ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d in order_lines:
                    print(f"**************************************************")
                    print(f"Item ID: {ol_i_id}")
                    print(f"Supplying Warehouse ID: {ol_supply_w_id}")
                    print(f"Quantity Ordered: {ol_quantity}")
                    print(f"Total Price: {ol_amount}")
                    print(f"Delivery Date and Time: {ol_delivery_d}")
                    
            else:
                print(f'No order for customer:({c_w_id},{c_d_id},{c_id})')
        else: 
            print(f'Cant find customer:({c_w_id},{c_d_id},{c_id})')

        # Commit transaction
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

        end_time = time.time()
        latency = (end_time - start_time) * 1000
        return latency

# Example usage
order_status(host="localhost", database="project", user="cs4224d", password="1234", c_w_id=1, c_d_id=1, c_id=1)
