import psycopg2
from psycopg2 import sql

def deliver(host, database, user, password, warehouse_id, carrier_id):
    conn = None
    try:
        # Establish connection to the database
        conn = psycopg2.connect(host=host, database=database, user=user, password=password)
        cur = conn.cursor()
        print('connection success to:')
        print(f'host: {host}  database:{database}  user:{user}')

        # Begin the transaction
        conn.autocommit = False

        # Processing steps for each district (1 to 10)
        for district_no in range(1, 11):
            # Step 1: Find the smallest order number with O_CARRIER_ID = null
            smallest_order_query = sql.SQL("""
                SELECT MIN(o_id) FROM orders 
                WHERE o_w_id = %s AND o_d_id = %s AND o_carrier_id IS NULL
            """).format(sql.Literal(warehouse_id), sql.Literal(district_no))
            cur.execute(smallest_order_query, (warehouse_id, district_no))
            smallest_order_number = cur.fetchone()[0]

            if smallest_order_number is not None:
                # Step 2: Update order X by setting O_CARRIER_ID to CARRIER_ID
                update_order_query = sql.SQL("""
                    UPDATE orders SET o_carrier_id = %s 
                    WHERE o_w_id = %s AND o_d_id = %s AND o_id = %s
                """).format(sql.Literal(warehouse_id), sql.Literal(district_no), sql.Literal(smallest_order_number))
                cur.execute(update_order_query, (carrier_id, warehouse_id, district_no, smallest_order_number))

                # Step 3: Update order-lines in X by setting OL_DELIVERY_D to current date and time
                update_order_lines_query = sql.SQL("""
                    UPDATE order_line SET ol_delivery_d = NOW() 
                    WHERE ol_w_id = %s AND ol_d_id = %s AND ol_o_id = %s
                """).format(sql.Literal(warehouse_id), sql.Literal(district_no), sql.Literal(smallest_order_number))
                cur.execute(update_order_lines_query, (warehouse_id, district_no, smallest_order_number))

                # Step 4: Update customer C
                update_customer_query = sql.SQL("""
                    UPDATE customer SET 
                        c_balance = c_balance + (SELECT SUM(ol_amount) FROM order_line WHERE ol_w_id = %s AND ol_d_id = %s AND ol_o_id = %s),
                        c_delivery_cnt = c_delivery_cnt + 1
                    WHERE c_w_id = %s AND c_d_id = %s AND c_id = (
                        SELECT o_c_id FROM orders 
                        WHERE o_w_id = %s AND o_d_id = %s AND o_id = %s
                    )
                """).format(sql.Literal(warehouse_id), sql.Literal(district_no), sql.Literal(smallest_order_number),
                            sql.Literal(warehouse_id), sql.Literal(district_no), sql.Literal(warehouse_id), 
                            sql.Literal(district_no), sql.Literal(smallest_order_number))
                cur.execute(update_customer_query, (warehouse_id, district_no, smallest_order_number,
                                                    warehouse_id, district_no, warehouse_id, 
                                                    district_no, smallest_order_number))

            print(f'finish district:{district_no}')

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
deliver(host="localhost", database="project", user="cs4224d", password="1234", 
        warehouse_id=1, carrier_id=1)
