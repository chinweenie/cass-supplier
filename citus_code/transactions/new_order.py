from datetime import datetime
from psycopg2 import sql

class newOrderProcessor:
    def __init__(self, conn, cursor):
        self.conn = conn
        self.cursor = cursor

    def new_order(self, w_id, d_id, c_id, num_items, item_numbers, supplier_warehouses, quantities):
        try:
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

                # Step 2: Update the district (W ID, D ID) by incrementing D NEXT O ID by one
                cursor.execute(sql.SQL("""
                    UPDATE district
                    SET d_next_o_id = d_next_o_id + 1
                    WHERE d_w_id = %s AND d_id = %s
                """), (w_id, d_id))

                # Step 3: Create a new order
                current_datetime = datetime.now()
                o_entry_d = current_datetime.strftime('%Y-%m-%d %H:%M:%S')
                o_all_local = 1 if all(s == w_id for s in supplier_warehouses) else 0

                cursor.execute(sql.SQL("""
                    INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local)
                    VALUES (%s, %s, %s, %s, %s, NULL, %s, %s)
                """), (next_order_number, d_id, w_id, c_id, o_entry_d, num_items, o_all_local))

                total_amount = 0

                # Step 5: Process each ordered item
                for i in range(num_items):
                    item_number = item_numbers[i]
                    supplier_warehouse = supplier_warehouses[i]
                    quantity = quantities[i]

                    # Step 5(a): Get the stock quantity for the item
                    cursor.execute(sql.SQL("""
                        SELECT s_quantity
                        FROM stock
                        WHERE s_w_id = %s AND s_i_id = %s
                    """), (supplier_warehouse, item_number))
                    stock_quantity = cursor.fetchone()

                    if not stock_quantity:
                        print(f"Item {item_number} not found in warehouse {supplier_warehouse}")
                        continue

                    stock_quantity = stock_quantity[0]

                    # Step 5(b): Adjust quantity based on stock level
                    adjusted_qty = stock_quantity - quantity
                    if adjusted_qty < 10:
                        adjusted_qty += 100

                    # Step 5(c): Update stock information
                    cursor.execute(sql.SQL("""
                        UPDATE stock
                        SET s_quantity = %s, s_ytd = s_ytd + %s, s_order_cnt = s_order_cnt + 1,
                            s_remote_cnt = s_remote_cnt + %s
                        WHERE s_w_id = %s AND s_i_id = %s
                    """), (adjusted_qty, quantity, 1 if supplier_warehouse != w_id else 0, supplier_warehouse, item_number))

                    # Step 5(f): Calculate ITEM AMOUNT
                    cursor.execute(sql.SQL("""
                        SELECT i_price
                        FROM item
                        WHERE i_id = %s
                    """), (item_number,))
                    item_price = cursor.fetchone()[0]
                    item_amount = quantity * item_price

                    # Step 5(g): Create a new order-line
                    cursor.execute(sql.SQL("""
                        INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id,
                                            ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d, ol_dist_info)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NULL, %s)
                    """), (next_order_number, d_id, w_id, i + 1, item_number, supplier_warehouse, quantity, item_amount, f'S DIST {d_id}'))

                    # Step 5(d): Update TOTAL AMOUNT
                    total_amount += item_amount

                # Step 6: Calculate the final total amount
                cursor.execute(sql.SQL("""
                    SELECT d_tax, w_tax
                    FROM district
                    WHERE d_w_id = %s AND d_id = %s
                """), (w_id, d_id))
                taxes = cursor.fetchone()
                d_tax, w_tax = taxes[0], taxes[1]

                cursor.execute(sql.SQL("""
                    SELECT c_discount
                    FROM customer
                    WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s
                """), (w_id, d_id, c_id))
                c_discount = cursor.fetchone()[0]

                total_amount = total_amount * (1 + d_tax + w_tax) * (1 - c_discount)

                # Output the results
                print("Customer Identifier:", (w_id, d_id, c_id))

                # Get customer information
                cursor.execute(sql.SQL("""
                    SELECT c_last, c_credit, c_discount
                    FROM customer
                    WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s
                """), (w_id, d_id, c_id))
                customer_info = cursor.fetchone()
                print("Last Name:", customer_info[0])
                print("Credit:", customer_info[1])
                print("Discount:", customer_info[2])

                # Get warehouse tax rate (W TAX) and district tax rate (D TAX)
                cursor.execute(sql.SQL("""
                    SELECT w_tax, d_tax
                    FROM warehouse
                    JOIN district ON d_w_id = w_id AND d_id = %s
                    WHERE w_id = %s
                """), (d_id, w_id))
                tax_info = cursor.fetchone()
                w_tax = tax_info[0]
                d_tax = tax_info[1]

                print("Warehouse Tax Rate (W TAX):", w_tax)
                print("District Tax Rate (D TAX):", d_tax)

                print("Order Number:", next_order_number)
                print("Entry Date:", o_entry_d)
                print("Number of Items (NUM ITEMS):", num_items)
                print("Total Amount for Order (TOTAL AMOUNT):", total_amount)

                print("\nOrdered Items:")
                for i in range(num_items):
                    item_number = item_numbers[i]
                    supplier_warehouse = supplier_warehouses[i]
                    quantity = quantities[i]
                    cursor.execute(sql.SQL("""
                        SELECT i_name
                        FROM item
                        WHERE i_id = %s
                    """), (item_number,))
                    item_name = cursor.fetchone()[0]
                    print(f"{item_number}:")
                    print("Item Name:", item_name)
                    print("Supplier Warehouse:", supplier_warehouse)
                    print("Quantity Ordered:", quantity)
                    print("Item Amount:", quantities[i] * item_price)
                    print("Stock Quantity (S QUANTITY):", stock_quantity)

        except Exception as e:
            print(f"Error: {str(e)}")
