# Script to process transactions in Citus

import psycopg2
from psycopg2 import sql

# Database connection parameters
db_params = {
    "dbname": "project",
    "user": "cs4224d",
    "password": "your_password",
    "host": "your_host",
    "port": "5100"
}

try:
    # Establish a database connection
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Input: Customer identifiers
    c_w_id = 1  # Replace with the actual values
    c_d_id = 2  # Replace with the actual values
    c_id = 3    # Replace with the actual values
    payment_amount = 500.0  # Replace with the actual payment amount

    # Transaction steps
    # 1. Update the warehouse (C W ID) by incrementing W YTD by PAYMENT
    cursor.execute(sql.SQL("""
        UPDATE warehouse
        SET w_ytd = w_ytd + %s
        WHERE w_id = %s
    """), (payment_amount, c_w_id))

    # 2. Update the district (C W ID, C D ID) by incrementing D YTD by PAYMENT
    cursor.execute(sql.SQL("""
        UPDATE district
        SET d_ytd = d_ytd + %s
        WHERE d_w_id = %s AND d_id = %s
    """), (payment_amount, c_w_id, c_d_id))

    # 3. Update the customer (C W ID, C D ID, C ID)
    # - Decrement C BALANCE by PAYMENT
    # - Increment C YTD PAYMENT by PAYMENT
    # - Increment C PAYMENT CNT by 1
    cursor.execute(sql.SQL("""
        UPDATE customer
        SET
            c_balance = c_balance - %s,
            c_ytd_payment = c_ytd_payment + %s,
            c_payment_cnt = c_payment_cnt + 1
        WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s
    """), (payment_amount, payment_amount, c_w_id, c_d_id, c_id))

    # Commit the transaction
    conn.commit()

    # Retrieve and print customer, warehouse, and district information
    cursor.execute(sql.SQL("""
        SELECT
            c.c_w_id, c.c_d_id, c.c_id,
            c.c_first, c.c_middle, c.c_last,
            c.c_street1, c.c_street2, c.c_city, c.c_state, c.c_zip,
            c.c_phone, c.c_since, c.c_credit, c.c_credit_lim, c.c_discount, c.c_balance,
            w.w_street1, w.w_street2, w.w_city, w.w_state, w.w_zip,
            d.d_street1, d.d_street2, d.d_city, d.d_state, d.d_zip
        FROM customer c
        JOIN district d ON c.c_w_id = d.d_w_id AND c.c_d_id = d.d_id
        JOIN warehouse w ON c.c_w_id = w.w_id
        WHERE c.c_w_id = %s AND c.c_d_id = %s AND c.c_id = %s
    """), (c_w_id, c_d_id, c_id))
    result = cursor.fetchone()

    if result:
        (
            c_w_id, c_d_id, c_id,
            c_first, c_middle, c_last,
            c_street1, c_street2, c_city, c_state, c_zip,
            c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance,
            w_street1, w_street2, w_city, w_state, w_zip,
            d_street1, d_street2, d_city, d_state, d_zip
        ) = result

        # 4. Payment amount PAYMENT
        print("Customer's information:")
        print(f"Customer ID: {c_w_id}, {c_d_id}, {c_id}")
        print(f"Name: {c_first} {c_middle} {c_last}")
        print(f"Address: {c_street1}, {c_street2}, {c_city}, {c_state}, {c_zip}")
        print(f"Phone: {c_phone}")
        print(f"Since: {c_since}")
        print(f"Credit: {c_credit}")
        print(f"Credit Limit: {c_credit_lim}")
        print(f"Discount: {c_discount}")
        print(f"Balance: {c_balance}")

        print("\nWarehouse's information:")
        print(f"Address: {w_street1}, {w_street2}, {w_city}, {w_state}, {w_zip}")

        print("\nDistrict's information:")
        print(f"Address: {d_street1}, {d_street2}, {d_city}, {d_state}, {d_zip}")

        print(f"\nPayment amount: {payment_amount}")

except Exception as e:
    print(f"Error: {str(e)}")
finally:
    # Close the cursor and connection
    if cursor:
        cursor.close()
    if conn:
        conn.close()
