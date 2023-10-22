from psycopg2 import sql

class stockProcessor:
    def __init__(self, conn, cursor):
        self.conn = conn
        self.cursor = cursor

    def process_stock_threshold(self, w_id, d_id, stock_threshold, last_orders_to_examine):
        try:
            # Step 1: Get the next available order number (N) for the district (W ID, D ID)
            self.cursor.execute(sql.SQL("""
                SELECT d_next_o_id
                FROM district
                WHERE d_w_id = %s AND d_id = %s
            """), (w_id, d_id))
            next_order_number = self.cursor.fetchone()

            if not next_order_number:
                print(f"No district found with W ID: {w_id} and D ID: {d_id}")
                return
            else:
                next_order_number = next_order_number[0]

                # Step 2: Get the set of items (S) from the last L orders for district (W ID, D ID)
                self.cursor.execute(sql.SQL("""
                    SELECT ol_i_id
                    FROM order_line
                    WHERE ol_w_id = %s AND ol_d_id = %s AND ol_o_id >= %s AND ol_o_id < %s
                """), (w_id, d_id, next_order_number - last_orders_to_examine, next_order_number))

                items_below_threshold = 0

                # Step 3: Count the number of items in S where its stock quantity at W ID is below the threshold (S QUANTITY < T)
                for row in self.cursor.fetchall():
                    ol_i_id = row[0]
                    self.cursor.execute(sql.SQL("""
                        SELECT s_quantity
                        FROM stock
                        WHERE s_w_id = %s AND s_i_id = %s
                    """), (w_id, ol_i_id))

                    stock_quantity = self.cursor.fetchone()
                    if stock_quantity and stock_quantity[0] < stock_threshold:
                        items_below_threshold += 1

                # Output the total number of items in S below the threshold
                print(f"Total number of items with stock quantity below {stock_threshold}: {items_below_threshold}")

        except Exception as e:
            print(f"Error: {str(e)}")
