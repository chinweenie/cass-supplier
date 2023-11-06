import psycopg2
from psycopg2 import sql
import time

def related_customer(host, database, user, password, c_w_id, c_d_id, c_id):
    conn = None
    start_time = time.time()
    try:
        # Establish connection to the database
        conn = psycopg2.connect(host=host, database=database, user=user, password=password)
        cur = conn.cursor()
        # print('connection success to:')
        # print(f'host: {host}  database:{database}  user:{user}')

        # # Begin transaction
        # conn.autocommit = False
        
        # # Step0: Lock the orders, customer, and order_line tables
        # lock_orders_query = sql.SQL("LOCK TABLE orders IN SHARE MODE")
        # cur.execute(lock_orders_query)

        # lock_customer_query = sql.SQL("LOCK TABLE customer IN SHARE MODE")
        # cur.execute(lock_customer_query)

        # lock_order_line_query = sql.SQL("LOCK TABLE order_line IN SHARE MODE")
        # cur.execute(lock_order_line_query)
        
        # need to split complex join down 
        # step1: Select candidate customer from customer table
        candidate_customer_query = sql.SQL("""
            select c_w_id, c_d_id, c_id
            from customers
            where c_w_id != %s
        """).format(sql.Literal(c_w_id))
        cur.execute(candidate_customer_query, (c_w_id,))
        candidate_customers = cur.fetchall()

        # step2: Select all orders for given customer
        orders_query = sql.SQL("""
            select o_id 
            from orders 
            where o_w_id = %s and o_d_id = %s and o_c_id = %s 
        """).format(sql.Literal(c_w_id), sql.Literal(c_d_id), sql.Literal(c_id))
        cur.execute(orders_query, (c_w_id, c_d_id, c_id))
        my_orders = cur.fetchall()
        
        # Output all related customer identifiers
        if candidate_customers is not None and my_orders is not None:
            # build a hashmap to reflect ol_i_ids to orders
            o_id = None
            order_to_lines_dict = {}
            for order_id in my_orders:     
                # actually ol_number is not needed, use set to store unique item id
                order_line_items_query = sql.SQL("""
                    select ol_i_id
                    from order_lines
                    where ol_w_id = %s and ol_d_id=%s and ol_o_id=%s
                """).format(sql.Literal(c_w_id), sql.Literal(c_d_id), sql.Literal(o_id))
                cur.execute(order_line_items_query, (c_w_id, c_d_id, order_id))
                my_order_line_items = cur.fetchall()
                order_to_lines_dict[order_id]=set(i for i in my_order_line_items)

            # print(order_to_lines_dict)

            ans = []
            # for each candidate_customer, use set calculation to get count of order_lines match the condition
            # if one order fits the condition(two order_lines fit), the customer is related  
            for customer_w_id, customer_d_id, customer_id in candidate_customers:
                flag = 0 
                related = 0 
                # build temp hashmap for reflection
                # print("search for candidate:", (customer_w_id, customer_d_id, customer_id)) 
                cur.execute(orders_query, (customer_w_id, customer_d_id, customer_id))
                this_orders = cur.fetchall()
                for this_order_id in this_orders:
                    # print("search for order:", this_order_id)
                    if related: 
                        flag = 1 
                        break 
                    else:
                        cur.execute(order_line_items_query,(customer_w_id, customer_d_id, this_order_id))
                        this_order_line_items = cur.fetchall()
                        tmp_reflection_set = set(i for i in this_order_line_items)
                        # use set intersection
                        for item_set in order_to_lines_dict.values():
                            if len(tmp_reflection_set&item_set)>=2:
                                related = 1
                                break 

                # add related customer identifier into answer
                if flag: 
                    ans.append((customer_w_id, customer_d_id, customer_id))

            print(f"Here are related customers of customer:{c_w_id}-{c_d_id}-{c_id}")
            for idt in ans:
                print(idt)

        else:
            print(f"Currently no orders or related customer for:{c_w_id}-{c_d_id}-{c_id}")

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

# # Example usage
# related_customer(host="localhost", database="project", user="cs4224d", password="1234", c_w_id=1, c_d_id=1, c_id=1)
