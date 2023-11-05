import psycopg2
from psycopg2 import sql

# Database connection parameters
db_params = {
    "dbname": "project",
    "user": "cs4224d",
    "password": "mypassword",
    "host": "localhost",
    "port": "5100"
}

try:
    # Establish a database connection
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Input: Warehouse number (W ID), District number (D ID), Number of last orders to be examined (L)
    w_id = 1  # Replace with the actual values
    d_id = 2  # Replace with the actual values
    last_orders_to_examine = 5  # Replace with the actual number of orders to examine (L)

    # Step 1: Get the next available order number (N) for the district (W ID, D ID)
    cursor.execute(sql.SQL("""
        SELECT d_next_o_id
        FROM district
        WHERE d_w_id = %s AND d_id = %s
    """), (w_id, d_id))
    next_order_number = cursor.fetchone()

    if not next_order_number:
        print(f"No district found with W ID: {w_id} and D ID: {d_id}")
    else:
        next_order_number = next_order_number[0]

        # Step 2: Get the set of last L orders (S) for district (W ID, D ID)
        cursor.execute(sql.SQL("""
            SELECT o_id, o_entry_d, c_first, c_middle, c_last
            FROM orders
            JOIN customer ON o_w_id = c_w_id AND o_d_id = c_d_id AND o_c_id = c_id
            WHERE o_w_id = %s AND o_d_id = %s AND o_id >= %s AND o_id < %s
        """), (w_id, d_id, next_order_number - last_orders_to_examine, next_order_number))

        orders = cursor.fetchall()

        # Create a dictionary to store popular items and order details
        popular_items = {}

        # Step 3: For each order x in S
        for order in orders:
            order_id, entry_d, c_first, c_middle, c_last = order
            cursor.execute(sql.SQL("""
                SELECT ol.ol_i_id, i.i_name, ol.ol_quantity
                FROM order_line ol
                JOIN item i ON ol.ol_i_id = i.i_id
                WHERE ol.ol_w_id = %s AND ol.ol_d_id = %s AND ol.ol_o_id = %s
            """), (w_id, d_id, order_id))

            order_lines = cursor.fetchall()

            for item_id, item_name, quantity_ordered in order_lines:
                if item_id not in popular_items:
                    popular_items[item_id] = {
                        "item_name": item_name,
                        "total_orders_with_item": 0
                    }

                if quantity_ordered > 0:
                    popular_items[item_id]["total_orders_with_item"] += 1

        # Output the results
        print("District Identifier:", (w_id, d_id))
        print("Number of Last Orders to be Examined:", last_orders_to_examine)

        for order in orders:
            order_id, entry_d, c_first, c_middle, c_last = order
            print("\nOrder Number & Entry Date and Time:", order_id, entry_d)
            print("Customer who Placed this Order:", f"{c_first} {c_middle} {c_last}")
            print("Popular Items in this Order:")

            for item_id, item_info in popular_items.items():
                item_name = item_info["item_name"]
                quantity_ordered = None

                cursor.execute(sql.SQL("""
                    SELECT ol_quantity
                    FROM order_line
                    WHERE ol_w_id = %s AND ol_d_id = %s AND ol_o_id = %s AND ol_i_id = %s
                """), (w_id, d_id, order_id, item_id))

                result = cursor.fetchone()
                if result:
                    quantity_ordered = result[0]

                if quantity_ordered is not None and quantity_ordered > 0:
                    print("  Item Name:", item_name)
                    print("  Quantity Ordered:", quantity_ordered)

        # Step 4: Calculate the percentage of examined orders that contain each popular item
        total_orders_examined = len(orders)

        print("\nPercentage of Orders with Popular Items:")
        for item_id, item_info in popular_items.items():
            item_name = item_info["item_name"]
            total_orders_with_item = item_info["total_orders_with_item"]
            percentage = (total_orders_with_item / total_orders_examined) * 100
            print(f"  Item Name: {item_name}")
            print(f"  Percentage of Orders: {percentage:.2f}%")

except Exception as e:
    print(f"Error: {str(e)}")
finally:
    # Close the cursor and connection
    if cursor:
        cursor.close()
    if conn:
        conn.close()
