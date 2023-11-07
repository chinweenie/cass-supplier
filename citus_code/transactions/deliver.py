import psycopg2
from psycopg2 import sql
import time 

def deliver(host, database, user, password, warehouse_id, carrier_id):
    conn = None
    cur = None
    start_time = time.time()
    try:
        # Establish connection to the database
        conn = psycopg2.connect(host=host, database=database, user=user, password=password)
        cur = conn.cursor()
        # print('connection success to:')
        # print(f'host: {host}  database:{database}  user:{user}')

        # # Begin the transaction
        # conn.autocommit = False

        # # Step0: Lock the orders, customer, and order_line tables
        # lock_orders_query = sql.SQL("LOCK TABLE orders IN EXCLUSIVE MODE")
        # cur.execute(lock_orders_query)

        # lock_customer_query = sql.SQL("LOCK TABLE customer IN EXCLUSIVE MODE")
        # cur.execute(lock_customer_query)

        # lock_order_line_query = sql.SQL("LOCK TABLE order_line IN EXCLUSIVE MODE")
        # cur.execute(lock_order_line_query)

        # Processing steps for each district (1 to 10)
        for district_no in range(1, 11):
            # Step 1: Find the smallest order number with O_CARRIER_ID = null
            smallest_order_query = sql.SQL("""
                select min(o_id) from orders
                where o_w_id = %s and o_d_id = %s and o_carrier_id is NULL
            """).format(sql.Literal(warehouse_id), sql.Literal(district_no))
            cur.execute(smallest_order_query, (warehouse_id, district_no))
            smallest_order_number = cur.fetchone()[0]

            if smallest_order_number is not None:
                # Step 2: Update order X by setting O_CARRIER_ID to CARRIER_ID
                update_order_query = sql.SQL("""
                    update orders set o_carrier_id = %s 
                    where o_w_id = %s and o_d_id = %s and o_id = %s
                """).format(sql.Literal(warehouse_id), sql.Literal(district_no), sql.Literal(smallest_order_number))
                cur.execute(update_order_query, (carrier_id, warehouse_id, district_no, smallest_order_number))

                # Step 3: Update order-lines in X by setting OL_DELIVERY_D to current date and time
                update_order_lines_query = sql.SQL("""
                    update order_lines set ol_delivery_d = NOW() 
                    where ol_w_id = %s and ol_d_id = %s and ol_o_id = %s
                """).format(sql.Literal(warehouse_id), sql.Literal(district_no), sql.Literal(smallest_order_number))
                cur.execute(update_order_lines_query, (warehouse_id, district_no, smallest_order_number))

                # Step 4: Update customer C
                update_customer_query = sql.SQL("""
                    update customers set 
                        c_balance = c_balance + (select sum(ol_amount) from order_line where ol_w_id = %s and ol_d_id = %s and ol_o_id = %s),
                        c_delivery_cnt = c_delivery_cnt + 1
                    where c_w_id = %s and c_d_id = %s and c_id = (
                        select o_c_id from orders
                        where o_w_id = %s and o_d_id = %s and o_id = %s
                    )
                """).format(sql.Literal(warehouse_id), sql.Literal(district_no), sql.Literal(smallest_order_number),
                            sql.Literal(warehouse_id), sql.Literal(district_no), sql.Literal(warehouse_id), 
                            sql.Literal(district_no), sql.Literal(smallest_order_number))
                cur.execute(update_customer_query, (warehouse_id, district_no, smallest_order_number,
                                                    warehouse_id, district_no, warehouse_id, 
                                                    district_no, smallest_order_number))

            # print(f'finish district:{district_no}')

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

        end_time = time.time()
        latency = (end_time - start_time) * 1000
        return latency

# # Example usage
# deliver(host="localhost", database="project", user="cs4224d", password="1234", 
#         warehouse_id=3, carrier_id=1)
