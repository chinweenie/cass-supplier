# Script to process transactions in Citus

import psycopg2
from psycopg2 import sql

# Database connection parameters
db_params = {
    "dbname": "project",
    "user": "cs4224d",
    "password": "1234",
    "host": "localhost",
    "port": "5100"
}

 # Establish a database connection
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

try:
    # Input: Warehouse number (W ID), District number (D ID), Stock threshold (T), Number of last orders to be examined (L)
    w_id = 1  # Replace with the actual values
    d_id = 2  # Replace with the actual values
    stock_threshold = 100  # Replace with the actual threshold value
    last_orders_to_examine = 5  # Replace with the actual number of orders to examine (L)

    # Step 1: Get the next available order number (N) for the districts (W ID, D ID)
    cursor.execute(sql.SQL("""
        SELECT d_next_o_id
        FROM districts
        WHERE d_w_id = %s AND d_id = %s
    """), (w_id, d_id))
    next_order_number = cursor.fetchone()

    if not next_order_number:
        print(f"No district found with W ID: {w_id} and D ID: {d_id}")
    else:
        next_order_number = next_order_number[0]

        # Step 2: Get the set of items (S) from the last L orders for districts (W ID, D ID)
        cursor.execute(sql.SQL("""
            SELECT ol_i_id
            FROM order_lines
            WHERE ol_w_id = %s AND ol_d_id = %s AND ol_o_id >= %s AND ol_o_id < %s
        """), (w_id, d_id, next_order_number - last_orders_to_examine, next_order_number))

        items_below_threshold = 0

        # Step 3: Count the number of items in S where its stock quantity at W ID is below the threshold (S QUANTITY < T)
        for row in cursor.fetchall():
            ol_i_id = row[0]
            cursor.execute(sql.SQL("""
                SELECT s_quantity
                FROM stocks
                WHERE s_w_id = %s AND s_i_id = %s
            """), (w_id, ol_i_id))

            stock_quantity = cursor.fetchone()
            if stock_quantity and stock_quantity[0] < stock_threshold:
                items_below_threshold += 1

        # Output the total number of items in S below the threshold
        print(f"Total number of items with stocks quantity below {stock_threshold}: {items_below_threshold}")

except Exception as e:
    print(f"Error: {str(e)}")
finally:
    # Close the cursor and connection
    if cursor:
        cursor.close()
    if conn:
        conn.close()