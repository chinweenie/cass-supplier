import psycopg2
from psycopg2 import sql
import time

def popular_item(host, database, port, user, password, w_id, d_id, last_orders_to_examine):

    # Database connection parameters
    db_params = {
        "database": database,
        "user": user,
        "password": password,
        "host": host,
        "port": port,
    }
    
    start_time = time.time()

    try:
        # Establish a database connection
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        # # Step 1: Get the next available order number (N) for the districts (W ID, D ID)
        # cursor.execute(sql.SQL("""
        #     SELECT d_next_o_id
        #     FROM districts
        #     WHERE d_w_id = %s AND d_id = %s
        # """), (w_id, d_id))
        # next_order_number = cursor.fetchone()

        # if not next_order_number:
        #     print(f"No district found with W ID: {w_id} and D ID: {d_id}")
        # else:
        #     next_order_number = next_order_number[0]

        #     # Step 2: Get the set of last L orders (S) for districts (W ID, D ID)
        #     cursor.execute(sql.SQL("""
        #         SELECT o_id, o_entry_d, c_first, c_middle, c_last
        #         FROM orders
        #         JOIN customers ON o_w_id = c_w_id AND o_d_id = c_d_id AND o_c_id = c_id
        #         WHERE o_w_id = %s AND o_d_id = %s AND o_id >= %s AND o_id < %s
        #     """), (w_id, d_id, next_order_number - last_orders_to_examine, next_order_number))

        #     orders = cursor.fetchall()

        #     # Create a dictionary to store popular items and order details
        #     popular_items = {}

        #     # Step 3: For each order x in S
        #     for order in orders:
        #         order_id, entry_d, c_first, c_middle, c_last = order
        #         cursor.execute(sql.SQL("""
        #             SELECT ol.ol_i_id, i.i_name, ol.ol_quantity
        #             FROM order_lines ol
        #             JOIN items i ON ol.ol_i_id = i.i_id
        #             WHERE ol.ol_w_id = %s AND ol.ol_d_id = %s AND ol.ol_o_id = %s
        #         """), (w_id, d_id, order_id))

        #         order_lines = cursor.fetchall()

        #         for item_id, item_name, quantity_ordered in order_lines:
        #             if item_id not in popular_items:
        #                 popular_items[item_id] = {
        #                     "item_name": item_name,
        #                     "total_orders_with_item": 0
        #                 }

        #             if quantity_ordered > 0:
        #                 popular_items[item_id]["total_orders_with_item"] += 1

        #     # Output the results
        #     print("District Identifier:", (w_id, d_id))
        #     print("Number of Last Orders to be Examined:", last_orders_to_examine)

        #     for order in orders:
        #         order_id, entry_d, c_first, c_middle, c_last = order
        #         print("\nOrder Number & Entry Date and Time:", order_id, entry_d)
        #         print("Customer who Placed this Order:", f"{c_first} {c_middle} {c_last}")
        #         print("Popular Items in this Order:")

        #         # for item_id, item_info in popular_items.items():
        #         #     item_name = item_info["item_name"]

        #         #     # Retrieve the correct quantity_ordered for each item
        #         #     cursor.execute(sql.SQL("""
        #         #         SELECT ol_quantity
        #         #         FROM order_lines
        #         #         WHERE ol_w_id = %s AND ol_d_id = %s AND ol_o_id = %s AND ol_i_id = %s
        #         #     """), (w_id, d_id, order_id, item_id))

        #         #     result = cursor.fetchone()
        #         #     if result:
        #         #         quantity_ordered = result[0]
        #         #     else:
        #         #         quantity_ordered = 0  # If no record is found, assume quantity is 0

        #         #     if quantity_ordered > 0:
        #         #         print("  Item Name:", item_name)
        #         #         print("  Quantity Ordered:", quantity_ordered)

        #         for item_id, item_info in popular_items.items():
        #             item_name = item_info["item_name"]
        #             quantity_ordered = None

        #             cursor.execute(sql.SQL("""
        #                 SELECT ol_quantity
        #                 FROM order_lines
        #                 WHERE ol_w_id = %s AND ol_d_id = %s AND ol_o_id = %s AND ol_i_id = %s
        #             """), (w_id, d_id, order_id, item_id))

        #             result = cursor.fetchone()
        #             if result:
        #                 quantity_ordered = result[0]

        #             if quantity_ordered is not None and quantity_ordered > 0:
        #                 print("  Item Name:", item_name)
        #                 print("  Quantity Ordered:", quantity_ordered)

        #     # Step 4: Calculate the percentage of examined orders that contain each popular item
        #     total_orders_examined = len(orders)

        #     print("\nPercentage of Orders with Popular Items:")
        #     for item_id, item_info in popular_items.items():
        #         item_name = item_info["item_name"]
        #         total_orders_with_item = item_info["total_orders_with_item"]
        #         percentage = (total_orders_with_item / total_orders_examined) * 100
        #         print(f"  Item Name: {item_name}")
        #         print(f"  Percentage of Orders: {percentage:.2f}%")
        
        # # Step 1: Get the next available order number (N) for the districts (W ID, D ID)
        # cursor.execute(sql.SQL("""
        #     SELECT d_next_o_id
        #     FROM districts
        #     WHERE d_w_id = %s AND d_id = %s
        # """), (w_id, d_id))
        # next_order_number = cursor.fetchone()

        # if not next_order_number:
        #     print(f"No district found with W ID: {w_id} and D ID: {d_id}")
        # else:
        #     next_order_number = next_order_number[0]

        #     # Step 2: Get the set of last L orders (S) for districts (W ID, D ID)
        #     cursor.execute(sql.SQL("""
        #         SELECT o.o_id, o.o_entry_d, c.c_first, c.c_middle, c.c_last
        #         FROM orders o
        #         JOIN customers c ON o.o_w_id = c.c_w_id AND o.o_d_id = c.c_d_id AND o.o_c_id = c.c_id
        #         WHERE o.o_w_id = %s AND o.o_d_id = %s AND o.o_id >= %s AND o.o_id < %s
        #     """), (w_id, d_id, next_order_number - last_orders_to_examine, next_order_number))

        #     orders = cursor.fetchall()

        #     # Create a dictionary to store popular items and order details
        #     popular_items = {}

        #     # Step 3: For each order x in S
        #     for order in orders:
        #         order_id, entry_d, c_first, c_middle, c_last = order
        #         cursor.execute(sql.SQL("""
        #             SELECT ol.ol_i_id, i.i_name, ol.ol_quantity
        #             FROM order_lines ol
        #             JOIN items i ON ol.ol_i_id = i.i_id
        #             WHERE ol.ol_w_id = %s AND ol.ol_d_id = %s AND ol.ol_o_id = %s
        #             AND ol.ol_quantity = (
        #                 SELECT MAX(ol2.ol_quantity)
        #                 FROM order_lines ol2
        #                 WHERE ol2.ol_w_id = ol.ol_w_id AND ol2.ol_d_id = ol.ol_d_id AND ol2.ol_o_id = ol.ol_o_id
        #             )
        #         """), (w_id, d_id, order_id))

        #         order_lines = cursor.fetchall()

        #         for item_id, item_name, quantity_ordered in order_lines:
        #             if item_id not in popular_items:
        #                 popular_items[item_id] = {
        #                     "item_name": item_name,
        #                     "total_orders_with_item": 0
        #                 }

        #             popular_items[item_id]["total_orders_with_item"] += 1

        #     # Output the results
        #     print("District Identifier:", (w_id, d_id))
        #     print("Number of Last Orders to be Examined:", last_orders_to_examine)

        #     for order in orders:
        #         order_id, entry_d, c_first, c_middle, c_last = order
        #         print("\nOrder Number & Entry Date and Time:", order_id, entry_d)
        #         print("Customer who Placed this Order:", f"{c_first} {c_middle} {c_last}")
        #         print("Popular Items in this Order:")

        #         for item_id, item_info in popular_items.items():
        #             item_name = item_info["item_name"]
        #             quantity_ordered = None

        #             cursor.execute(sql.SQL("""
        #                 SELECT ol_quantity
        #                 FROM order_lines
        #                 WHERE ol_w_id = %s AND ol_d_id = %s AND ol_o_id = %s AND ol_i_id = %s
        #             """), (w_id, d_id, order_id, item_id))

        #             result = cursor.fetchone()
        #             if result:
        #                 quantity_ordered = result[0]

        #             if quantity_ordered is not None and quantity_ordered > 0:
        #                 print("  Item Name:", item_name)
        #                 print("  Quantity Ordered:", quantity_ordered)

        #     # Step 4: Calculate the percentage of examined orders that contain each popular item
        #     total_orders_examined = len(orders)

        #     print("\nPercentage of Orders with Popular Items:")
        #     for item_id, item_info in popular_items.items():
        #         item_name = item_info["item_name"]
        #         total_orders_with_item = item_info["total_orders_with_item"]
        #         percentage = (total_orders_with_item / total_orders_examined) * 100
        #         print(f"  Item Name: {item_name}")
        #         print(f"  Percentage of Orders: {percentage:.2f}%")

        # Step 1: Get the next available order number (N) for the districts (W ID, D ID)
        cursor.execute(sql.SQL("""
            SELECT d_next_o_id
            FROM districts
            WHERE d_w_id = %s AND d_id = %s
        """), (w_id, d_id))
        next_order_number = cursor.fetchone()

        if not next_order_number:
            print(f"No district found with W ID: {w_id} and D ID: {d_id}")
            return

        next_order_number = next_order_number[0]

        # Step 2: Get the set of last L orders (S) for districts (W ID, D ID)
        cursor.execute(sql.SQL("""
            SELECT o.o_id, o.o_entry_d, c.c_first, c.c_middle, c.c_last
            FROM orders o
            JOIN customers c ON o.o_w_id = c.c_w_id AND o.o_d_id = c.c_d_id AND o.o_c_id = c.c_id
            WHERE o.o_w_id = %s AND o.o_d_id = %s AND o.o_id >= %s AND o.o_id < %s
        """), (w_id, d_id, next_order_number - last_orders_to_examine, next_order_number))

        orders = cursor.fetchall()

        # Create a dictionary to store popular items and order details
        popular_items = {}

        # Create a list to store the order IDs in S
        order_ids_in_S = [order[0] for order in orders]

        # Step 3: Fetch all order lines for orders in S
        cursor.execute(sql.SQL("""
            SELECT ol.ol_o_id, ol.ol_i_id, i.i_name, ol.ol_quantity
            FROM order_lines ol
            JOIN items i ON ol.ol_i_id = i.i_id
            WHERE ol.ol_w_id = %s AND ol.ol_d_id = %s AND ol.ol_o_id IN %s
        """), (w_id, d_id, tuple(order_ids_in_S)))

        order_lines = cursor.fetchall()

        # Organize order lines by order ID
        order_lines_by_order = {}
        for order_line in order_lines:
            order_id, item_id, item_name, quantity_ordered = order_line
            if order_id not in order_lines_by_order:
                order_lines_by_order[order_id] = []
            order_lines_by_order[order_id].append((item_id, item_name, quantity_ordered))

        # Step 4: Process orders and calculate popularity
        for order in orders:
            order_id, entry_d, c_first, c_middle, c_last = order
            print("\nOrder Number & Entry Date and Time:", order_id, entry_d)
            print("Customer who Placed this Order:", f"{c_first} {c_middle} {c_last}")
            print("Popular Items in this Order:")

            order_lines = order_lines_by_order.get(order_id, [])

            for item_id, item_name, quantity_ordered in order_lines:
                if item_id not in popular_items:
                    popular_items[item_id] = {
                        "item_name": item_name,
                        "total_orders_with_item": 0
                    }

                if quantity_ordered > 0:
                    print("  Item Name:", item_name)
                    print("  Quantity Ordered:", quantity_ordered)
                    popular_items[item_id]["total_orders_with_item"] += 1

        # Step 5: Calculate the percentage of examined orders that contain each popular item
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

        end_time = time.time()
        latency = (end_time - start_time) * 1000
        return latency


# parameters link to citus 
host="localhost"
database="citus_project"
user="cs4224d"
password="1234"
port="5100"

transaction_message = (0,1,4,35)

w_id, d_id, last_orders_to_examine = transaction_message[1], transaction_message[2], int(transaction_message[3])

latency = popular_item(host, database, port, user, password, w_id, d_id, last_orders_to_examine)

print("Duration cost by this task: ", latency)