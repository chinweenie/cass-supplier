import os
import pandas as pd
import time
import statistics
import sys
import csv

from decimal import Decimal
from cassandra.query import BatchStatement, SimpleStatement, ValueSequence
import pandas as pd
from cassandra.policies import TokenAwarePolicy, RoundRobinPolicy, DowngradingConsistencyRetryPolicy
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, ExecutionProfile
from datetime import datetime


def format_res(*results_sets):
    output = []
    for results in results_sets:
        if isinstance(results, dict):  # Handle dictionary directly
            for key, value in results.items():
                output.append(f"{key}: {value}")
        elif isinstance(results, pd.DataFrame):  # Handle dataframe
            str = results.to_string(index=False)
            output.append(str)
        else:  # Handle namedtuples
            for row in results:
                for column_name, value in row._asdict().items():
                    output.append(f"{column_name}: {value}")
    output.append("----\n")
    return "\n".join(output)


def process_o(db, values, output_file):
    executed = False
    if len(values) < 4:
        print("Not enough arguments of O txn!")
        return executed
    c_w_id = int(values[1])
    c_d_id = int(values[2])
    c_id = int(values[3])

    last_order_by_customer_statement = SimpleStatement(
        """SELECT * FROM orders_by_customer WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s LIMIT 1""")
    res = db.execute(last_order_by_customer_statement, (c_w_id, c_d_id, c_id)).one()
    if not res:
        output_file.write("no orders by customer found")
        return executed

    last_order_id = res.o_id
    last_order_items_statement = SimpleStatement(
        """SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_lines WHERE ol_w_id = %s 
        AND ol_d_id = %s AND ol_o_id = %s""")
    order_items_res = db.execute(last_order_items_statement, (c_w_id, c_d_id, last_order_id))
    if not order_items_res:
        output_file.write("no order items found")
        return executed
    formatted_res = format_res(
        {"o_id": last_order_id, "o_entry_d": res.o_entry_d, "o_carrier_id": res.o_carrier_id}, order_items_res)
    output_file.write(formatted_res)

    executed = True
    return executed


def process_p(db, values, output_file):
    executed = False
    if len(values) < 5:
        output_file.write("Not enough arguments of P txn!")
        return executed

    c_w_id = int(values[1])
    c_d_id = int(values[2])
    c_id = int(values[3])
    payment = Decimal(values[4])

    # read and update c_ytd
    user_res = db.execute(customers_statement, (c_w_id, c_d_id, c_id)).one()
    print(user_res)
    if not user_res:
        output_file.write(f"customer not found")
        return executed

    old_balance = Decimal(user_res.c_balance)
    new_balance = old_balance - payment

    new_ytd_balance = Decimal(user_res.c_ytd_payment) + payment
    new_payment_cnt = user_res.c_payment_cnt + 1

    update_customer_statement = db.prepare(
        "UPDATE customers SET c_balance = ?, c_ytd_payment = ?, c_payment_cnt = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?")
    db.execute(update_customer_statement, (new_balance, new_ytd_balance, new_payment_cnt, c_w_id, c_d_id, c_id))


    # read and update w_ytd
    warehouses = warehouses_statement
    # warehouses.consistency_level = ConsistencyLevel.ONE
    warehouses_res = db.execute(warehouses, (c_w_id,)).one()
    if not warehouses_res:
        output_file.write(f"warehouse not found")
        return executed

    new_warehouse_ytd = Decimal(warehouses_res.w_ytd) + payment

    update_warehouse_statement = db.prepare("UPDATE warehouses SET w_ytd = ? WHERE w_id = ?")
    db.execute(update_warehouse_statement, (new_warehouse_ytd, c_w_id))

    districts = districts_statement
    # districts.consistency_level = ConsistencyLevel.ONE
    districts_res = db.execute(districts, (c_w_id, c_d_id)).one()
    if not districts_res:
        output_file.write(f"district not found")
        return executed

    new_district_ytd = Decimal(districts_res.d_ytd) + payment

    update_district_statement = db.prepare("UPDATE districts SET d_ytd = ? WHERE d_w_id = ? AND d_id = ?")
    db.execute(update_district_statement, (new_district_ytd, c_w_id, c_d_id))


    # delete_query = db.prepare(
    #     "DELETE FROM top_balances WHERE c_w_id = ? AND c_balance = ? AND c_d_id = ? AND c_id = ?")
    # batch.add(delete_query, (c_w_id, old_balance, c_d_id, c_id))
    #
    # insert_query = db.prepare(
    #     "INSERT INTO top_balances (c_balance, c_w_id, c_id, c_d_id, c_name, c_w_name, c_d_name) VALUES (?, ?, ?, ?, ?, ?, ?)")
    # batch.add(insert_query, (
    #     new_balance, c_w_id, c_id, c_d_id, user_res.c_name, warehouses_res.w_name,
    #     districts_res.d_name))

    #except Exception as e:
    #    output_file.write(f"Error occurred: {e}")
    #    return executed

    formatted_res = format_res({'c_w_id': c_w_id,
                                'c_d_id': c_d_id,
                                'c_id': c_id,
                                'c_name': user_res.c_name,
                                'c_address': user_res.c_address,
                                'c_phone': user_res.c_phone,
                                'c_since': user_res.c_since,
                                'c_credit': user_res.c_credit,
                                'c_credit_limit': user_res.c_credit_lim,
                                'c_discount': user_res.c_discount,
                                'c_balance': new_balance,
                                'w_address': warehouses_res.w_address,
                                'd_address': districts_res.d_address,
                                'payment': payment})
    output_file.write(formatted_res)
    executed = True
    return executed


def process_t(db, values, output_file):
    executed = False
    warehouses_res = db.execute(SimpleStatement("""SELECT w_id, w_name FROM warehouses;"""))
    statement = db.prepare(f"""SELECT c_name, c_w_name, c_d_name, c_balance, c_w_id, c_d_id, c_id FROM top_balances WHERE c_w_id = ? LIMIT 10;""")
    # statement.consistency_level = ConsistencyLevel.ONE
    customers_list = []
    for warehouse in warehouses_res:
        res = db.execute(statement, (warehouse.w_id,))
        customers_list.extend(res)

    customers_list = sorted(customers_list, key=lambda x: x.c_balance, reverse=True)
    top_ten = customers_list[:10]
    formatted_res = format_res(top_ten)
    output_file.write(formatted_res)
    executed = True
    return executed


def process_s(db, values, output_file):
    executed = False
    if len(values) < 5:
        print("Not enough arguments of S txn!")
        return executed
    w_id = int(values[1])
    d_id = int(values[2])
    T = Decimal(values[3])
    L = int(values[4])

    last_order_num_lookup_statement = districts_statement
    district_res = db.execute(last_order_num_lookup_statement, (w_id, d_id)).one()
    last_order_num = district_res.d_next_o_id

    items_lookup_statement = "select ol_i_id from order_lines where ol_w_id = %s and ol_d_id = %s and ol_o_id >= %s"
    items_res = db.execute(items_lookup_statement, (w_id, d_id, last_order_num - L))

    items = set()
    for item in items_res:
        items.add(item.ol_i_id)

    s_res = db.execute("select s_quantity from stocks where s_w_id = %s and s_i_id IN %s", parameters=[w_id, ValueSequence(items)])
    count = 0
    for s in s_res:
        if s.s_quantity < T:
            count += 1
    formatted_res = format_res({'number of items with low stock quantity':count})
    output_file.write(formatted_res)
    executed = True

    return executed


def process_i(db, values, output_file):
    executed = False
    if len(values) < 4:
        print("You must provide exactly 4 arguments!")
        return executed

    w_id = int(values[1])
    d_id = int(values[2])
    L = int(values[3])

    last_order_num_lookup_statement = districts_statement
    N_res = db.execute(last_order_num_lookup_statement, (w_id, d_id)).one()
    N = N_res.d_next_o_id

    last_L_order_lookup_statement = "select o_id, o_entry_d, c_name, i_id, i_name, ol_quantity from popular_item_transaction where w_id=%s and d_id=%s and o_id >= %s"
    S_res = db.execute(last_L_order_lookup_statement, (w_id, d_id, N - L))
    S_df = pd.DataFrame(list(S_res))

    # s_without_id_df = S_df.drop(['i_id'], axis=1)

    p_df = S_df[['i_id', 'i_name']]
    p_df = p_df.value_counts().reset_index().rename(columns={"count": "percentage"})
    p_df['percentage'] = (p_df['percentage'] * 100 / L).round(2)
    # p_df = p_df.drop(['i_id'], axis=1)

    formatted_res = format_res({"W_ID": w_id, "D_ID": d_id, "L": L},
                               S_df,
                               p_df)
    output_file.write(formatted_res)

    executed = True
    return executed


# Processes the oldest undelivered order for all 10 districts of each warehouse
# The oldest undelivered order is determined by the smallest O_ID
def process_d(db, values, output_file):
    executed = False
    if len(values) < 3:
        print("Not enough arguments for txd D!")
        return executed

    w_id = int(values[1])
    carrier_id = int(values[2])

    # prepared statement
    oldest_undel_order_per_wid_did = db.prepare("""SELECT * FROM undelivered_orders_by_warehouse_district WHERE 
            o_w_id = ? AND o_d_id = ? LIMIT 1""")

    oldest_undelivered_orders = []
    # d_id has values from 1-10 for each warehouse
    for d_id in range(1, 11):
        # oldest undelivered order is the first result of the select as o_id is ordered in ascending
        oldest_undelivered_order = db.execute(oldest_undel_order_per_wid_did, [w_id, d_id]).one()
        oldest_undelivered_orders.append(oldest_undelivered_order)


    update_carrier_stmt = db.prepare("UPDATE orders SET o_carrier_id = ? WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?")
    update_carrier_stmt.consistency_level = ConsistencyLevel.QUORUM
    update_orders_by_customer_stt = db.prepare("UPDATE orders_by_customer SET o_carrier_id = ? WHERE c_w_id = ? AND "
                                               "c_d_id = ? AND c_id = ? AND o_id = ? ")
    update_orders_by_customer_stt.consistency_level = ConsistencyLevel.QUORUM
    get_order_lines_stmt = db.prepare("SELECT * FROM order_lines WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?")
    update_order_lines_stmt = db.prepare("""UPDATE order_lines SET ol_delivery_d = ? WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? 
            AND ol_number = ?""")
    update_order_lines_stmt.consistency_level = ConsistencyLevel.QUORUM
    get_customer_row_values = db.prepare(
        "SELECT c_balance, c_delivery_cnt, c_w_id, c_d_id, c_id, c_name FROM customers WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?")
    update_customer_stmt = db.prepare(
        "UPDATE customers SET c_balance = ?, c_delivery_cnt = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?")
    update_customer_stmt.consistency_level = ConsistencyLevel.QUORUM
    # update carrier_id in orders for each delivered order
    # update all order lines for the order to current date and time
    # update customer balance by total price of the orders
    # update customer delivery count by 1
    for d_id in range(1, 11):
        order = oldest_undelivered_orders[d_id - 1]

        if (order == None):
            continue

        o_w_id = int(order.o_w_id)
        o_d_id = int(order.o_d_id)
        o_id = int(order.o_id)
        o_c_id = int(order.o_c_id)

        # update carrier_id in orders table
        db.execute_async(update_carrier_stmt, [carrier_id, o_w_id, o_d_id, o_id])
        # update carrier_id in orders_by_customer table
        db.execute_async(update_orders_by_customer_stt, (carrier_id, o_w_id, o_d_id, o_c_id, o_id))

        # get order_lines corresponding to the order
        order_lines = db.execute(get_order_lines_stmt, [o_w_id, o_d_id, o_id])

        # update all order_lines to current date and time
        curr_timestamp = datetime.now()
        total_order_amount = Decimal(0)
        new_delivery_count = 0

        for ol in order_lines:
            ol_number = ol.ol_number

            # update order delivery date time
            db.execute_async(update_order_lines_stmt, [curr_timestamp, o_w_id, o_d_id, o_id, ol_number])

            # calculate total sum of order
            total_order_amount = total_order_amount + Decimal(ol.ol_amount)

        # get customer balance and delivery count values from table. searched by unique key so only 1 result
        customer_values = db.execute(get_customer_row_values, [o_w_id, o_d_id, o_c_id]).one()

        if (customer_values == None):
            return executed

        total_order_amount = total_order_amount + Decimal(customer_values.c_balance)
        new_delivery_count += 1
        # update customer tables with total amounts and delivery count
        db.execute_async(update_customer_stmt, [total_order_amount, new_delivery_count, o_w_id, o_d_id, o_c_id])

    delete_order_stmt = db.prepare("""DELETE FROM undelivered_orders_by_warehouse_district 
            WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?""")
    delete_order_stmt.consistency_level = ConsistencyLevel.QUORUM
    # delete delivered orders from undelivered orders table
    for order in oldest_undelivered_orders:
        if (order == None):
            continue
        #print(f"deleting w: {order.o_w_id}, d: {order.o_d_id}, o: {order.o_id}")
        db.execute_async(delete_order_stmt, [order.o_w_id, order.o_d_id, order.o_id])

    executed = True
    return executed


# Finds related customers
def process_r(db, values, output_file):
    executed = False
    w_id = int(values[1])
    d_id = int(values[2])
    c_id = int(values[3])

    # write customer identifier
    output_file.write(f"C_W_ID: {w_id}, C_D_ID: {d_id}, C_ID: {c_id}")

    get_items_stmt = db.prepare(
        "SELECT w_id, d_id, c_id, o_id, i_id FROM orders_by_warehouse_district_customer WHERE w_id = ? AND d_id = ? AND c_id = ?")
    items_table = db.execute(get_items_stmt, [w_id, d_id, c_id])

    executed = True

    items_df = pd.DataFrame([d for d in items_table])

    if (len(items_df) < 2):
        # customer does not have enough items to find related customers, write empty
        print("customer in process_r has not bought any items")
        return executed
    
    # print(items_df)

    temp_items_df = items_df.rename(columns={'i_id': 'i2_id'})
    items_df = items_df.rename(columns={'i_id': 'i1_id'})

    # join 2 items table to get a table with 2 items per row given an order
    two_items_df = items_df.merge(temp_items_df, on=['w_id', 'd_id', 'c_id', 'o_id'], how='inner')

    # filter rows with 2 of the same items
    two_items_df = two_items_df[two_items_df['i1_id'] != two_items_df['i2_id']]

    two_items_df = two_items_df.rename(columns={'w_id': 'w1_id', 'd_id': 'd1_id',
                                                'c_id': 'c1_id', 'o_id': 'o1_id'})
    
    items_columns = ['w_id', 'd_id', 'c_id', 'o_id', 'i_id']
    customer_items_df = pd.DataFrame(columns=items_columns)
    get_rcust_stmt = db.prepare("""SELECT w_id, d_id, c_id, o_id, i_id FROM orders_by_warehouse_district_customer WHERE 
        i_id = ?""")

    for index, row in items_df.iterrows():
        item = row['i1_id']
        res_set = db.execute(get_rcust_stmt, [item])
        cust_items = pd.DataFrame([d for d in res_set])
        customer_items_df = pd.concat([customer_items_df, cust_items], ignore_index=True)
       
    customer_items_df = customer_items_df.drop_duplicates()

    # remove all customers with warehouse id same as original customer
    all_cust_items_df = customer_items_df.loc[customer_items_df['w_id'] != w_id]
    temp_all_cust_df = all_cust_items_df.rename(columns={'i_id': 'i2_id'})
    all_cust_df = all_cust_items_df.rename(columns={'i_id': 'i1_id'})
    
    # join 2 items table to get a table with 2 items per row given an order
    all_cust_2_items_df = all_cust_df.merge(temp_all_cust_df, on=['w_id', 'd_id', 'c_id', 'o_id'], how='inner')

    # filter rows with 2 of the same items
    all_cust_2_items_df = all_cust_2_items_df[all_cust_2_items_df['i1_id'] != all_cust_2_items_df['i2_id']]
    
    temp_2_items_df = all_cust_2_items_df.rename(columns={'w_id': 'w2_id', 'd_id': 'd2_id',
        'c_id': 'c2_id', 'o_id': 'o2_id'})

    r_c_i = two_items_df.merge(temp_2_items_df, on=['i1_id', 'i2_id'], how='inner')

    r_c_i = r_c_i.drop_duplicates(subset=['w2_id', 'd2_id', 'c2_id'])

    # write related customer identifiers
    for index, row in r_c_i.iterrows():
        # print(f"C_W_ID: {row['w2_id']}, C_D_ID: {row['d2_id']}, C_ID: {row['c2_id']}")
        output_file.write(f"C_W_ID: {row['w2_id']}, C_D_ID: {row['d2_id']}, C_ID: {row['c2_id']}")

    return executed


# txn 2.1
def process_n(db, values, output_file):
    executed = False
    if len(values[0]) < 5:
        print("You must provide exactly 5 arguments!")
        return executed 

    c_id = int(values[0][1])
    w_id = int(values[0][2])
    d_id = int(values[0][3])
    m = int(values[0][4])
    ols = values[1:]

    # prepare statements here
    last_order_num_lookup_statement = districts_statement
        # db.prepare("SELECT * FROM districts WHERE d_w_id = ? AND d_id = ?")
    last_order_num_lookup_statement.consistency_level = ConsistencyLevel.QUORUM # make sure having the latest order num
    last_L_order_lookup_statement = db.prepare("UPDATE districts SET d_next_o_id = ? WHERE d_w_id = ? AND d_id = ?")
    create_order_statement = db.prepare("INSERT INTO orders (o_w_id,o_d_id,o_id,o_c_id,o_ol_cnt,o_carrier_id,o_all_local,o_entry_d) \
                                            VALUES (?, ?, ?, ?, ?, ?, ?, ?) \
                                            IF NOT EXISTS") # make sure orders are not overwriting
    create_order_statement.consistency_level = ConsistencyLevel.QUORUM
    all_local = 1
    total_item_quantity = 0
    for item in ols:
        if item[1] != w_id :
            all_local = 0
            break
        total_item_quantity += item[2]

    o_entry_date = datetime.now()

    not_success = True
    while not_success:
        d_res = db.execute(last_order_num_lookup_statement, (w_id, d_id)).one()
        N, d_tax = d_res.d_next_o_id, d_res.d_tax

        # update d_next_o_id column by 1
        db.execute(last_L_order_lookup_statement, (N+1, w_id, d_id))

        # create a new order
        o_res = db.execute(create_order_statement, (w_id, d_id, N, c_id, total_item_quantity, None, all_local, o_entry_date))
        is_order_create_success = o_res.one().applied
        not_success = False if is_order_create_success else True

    total_amount = 0
    most_popular_order_line_quantity = -1
    popular_ol_info = []
    output = []

    # for every item
    # prepare statements here
    stock_lookup_statement = db.prepare("SELECT * FROM stocks WHERE s_w_id = ? AND s_i_id = ?")
    price_lookup_statement = db.prepare("SELECT * FROM items WHERE i_id = ?")
    stock_update_statement = db.prepare("UPDATE stocks set s_quantity = ?, s_YTD = ?, s_order_cnt=?, s_remote_cnt=? WHERE s_w_id = ? AND s_i_id = ?") 
    create_order_line_statement = db.prepare("INSERT INTO order_lines \
        (ol_o_id,ol_d_id,ol_w_id,ol_number,ol_i_id,ol_supply_w_id,ol_quantity,ol_amount,ol_delivery_d,ol_dist_info) \
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)") 
    update_txn8_statement = db.prepare("INSERT INTO orders_by_warehouse_district_customer \
            (w_id,d_id,c_id,o_id,ol_number,i_id) \
            VALUES (?, ?, ?, ?, ?, ?)") 

    for index, item in enumerate(ols):
        i_id = int(item[0])
        s_w_id = int(item[1])
        quantity = int(item[2])

        s_res = db.execute(stock_lookup_statement, (s_w_id, i_id)).one()
        s_q, s_ytd, s_order_cnt, s_remote_cnt =s_res.s_quantity, s_res.s_ytd, s_res.s_order_cnt, s_res.s_remote_cnt 

        adjusted_q = s_q - quantity 
        if adjusted_q < 10:
            adjusted_q += 100
        s_ytd +=quantity 
        s_order_cnt += 1
        if s_w_id != w_id:
            s_remote_cnt += 1

        db.execute_async(stock_update_statement, (adjusted_q, s_ytd, s_order_cnt, s_remote_cnt, s_w_id, i_id))

        # item amount
        i_res = db.execute(price_lookup_statement, ([i_id])).one()
        price, i_name = i_res.i_price, i_res.i_name
        item_amount = quantity * price
        total_amount += item_amount

        # batch every order line
        batch = BatchStatement()
        # create new order line
        batch.add(create_order_line_statement, 
                                 (N, d_id, w_id, index+1, i_id, s_w_id, quantity, item_amount, None, "S_DIST_"+str(d_id)))
        # update for txn 8:
        batch.add(update_txn8_statement, (w_id, d_id, c_id, N, index+1, i_id))
        db.execute(batch)

        # record for txn 6
        if (quantity > most_popular_order_line_quantity):
            popular_ol_info = [[index+1, i_id, quantity, i_name]]
            most_popular_order_line_quantity = quantity
        elif(quantity == most_popular_order_line_quantity):
            popular_ol_info.append([index+1, i_id, quantity, i_name])

        output.append([i_id, i_name, s_w_id, quantity, item_amount, adjusted_q])

    w_lookup_statement = warehouses_statement
    # "SELECT * FROM warehouses WHERE w_id = %s"
    w_res_future = db.execute_async(w_lookup_statement, ([w_id]))
    c_lookup_statement = customers_statement
        # "SELECT * FROM customers WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s"
    c_res_future = db.execute_async(c_lookup_statement, ([w_id, d_id, c_id]))

    # update for txn 3:
    update_txn3_statement = "INSERT INTO undelivered_orders_by_warehouse_district \
                (o_w_id,o_d_id,o_id,o_c_id, o_carrier_id) \
                VALUES (%s, %s, %s, %s, %s)"
    db.execute_async(update_txn3_statement, ([w_id, d_id, N, c_id, None])) 

    # update for txn 4:
    update_txn4_statement = "INSERT INTO orders_by_customer \
                (c_w_id,c_d_id,c_id,o_id,o_carrier_id,o_entry_d) \
                VALUES (%s, %s, %s, %s, %s, %s)"
    db.execute_async(update_txn4_statement, ([w_id, d_id, c_id, N, None, o_entry_date])) 

    w_tax = w_res_future.result().one().w_tax 
    c_res = c_res_future.result().one()
    c_discount, c_name, c_credit = c_res.c_discount, c_res.c_name, c_res.c_credit
    c_lastname = c_name[2]
    total_amount = round(total_amount * (1 + d_tax + w_tax) * (1 - c_discount),2)

    # update for txn 6:
    update_txn6_statement = db.prepare("INSERT INTO popular_item_transaction \
            (w_id,d_id,o_id,o_entry_d,c_name,ol_number,i_id,ol_quantity,i_name) \
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
    for _, ol in enumerate(popular_ol_info):
        db.execute_async(update_txn6_statement, (w_id, d_id, N, o_entry_date, c_name, ol[0], ol[1], ol[2], ol[3]))  


    # output
    df = pd.DataFrame(output, columns =['item_number', 'i_name', 'supplier_warehouse', 'quantity', 'ol_amount', 's_quantity'])  

    formatted_res = format_res({"W_ID": w_id, "D_ID": d_id, "C_ID": c_id, "lastname": c_lastname, "credit": c_credit, "discount": c_discount},
                                {"warehose tax rate": w_tax, "district tax rate": d_tax},
                                {"order number": N, "entry date": o_entry_date},
                                {"number of items": len(ols), "total amount": total_amount},
                                df)
    output_file.write(formatted_res)
    executed = True
    return executed

if __name__ == '__main__':
    print(sys.argv)
    if len(sys.argv) != 3:
        print("You must provide exactly 2 arguments!")
        sys.exit(1)

    ip_address = sys.argv[1]
    filename = sys.argv[2]
    client = filename.split('.')[0]

    print(f"Received IP Address: {ip_address}")
    print(f"Received filename: {filename}")

    cluster_profile = ExecutionProfile(
        load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()),
        consistency_level=ConsistencyLevel.QUORUM,
        retry_policy=DowngradingConsistencyRetryPolicy(),
        request_timeout=3000
    )

    cluster = Cluster([ip_address])
    session = cluster.connect()
    session.set_keyspace('supplier')
    # session.default_timeout = 60
    directory = "/temp/teamd-cass/apache-cassandra-4.1.3/bin/xact_files/"
    shared_dir = "/home/stuproj/cs4224d/cass_log/"
    client_dir="/temp/teamd-cass/client/"

    districts_statement = session.prepare("""SELECT * FROM districts WHERE 
            d_w_id = ? AND d_id = ?""")
    warehouses_statement = session.prepare("""SELECT * FROM warehouses WHERE w_id = ?""")
    customers_statement = session.prepare("""SELECT * FROM customers WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?""")

    total_transactions = 0
    latencies = []  # List to store latency of each transaction
    start_time = time.time()
    try:
        # stdout for each client
        with open(f"{client_dir}stdout_client{filename}", 'w') as output_file:
            dir_filename = os.path.join(directory, filename)
            with open(dir_filename, 'r') as file:
                for line in file:
                    print(line.strip())
                    txn_start_time = time.time()

                    txn_keys = line.strip().split(',')
                    is_successfully_executed = False
                    if txn_keys[0].lower() == 'n' or txn_keys[0].isnumeric():
                        # handle m more lines
                        if txn_keys[0].lower() == 'n':
                            m = int(txn_keys[4])
                            txn_inputs = [txn_keys]
                        else:
                            txn_inputs.append(txn_keys)
                            m -= 1
                            if m == 0:
                                is_successfully_executed = process_n(session, txn_inputs, output_file)
                    if txn_keys[0].lower() == 'p':
                        is_successfully_executed = process_p(session, txn_keys, output_file)
                    if txn_keys[0].lower() == 't':
                        is_successfully_executed = process_t(session, txn_keys, output_file)
                    if txn_keys[0].lower() == 's':
                        is_successfully_executed = process_s(session, txn_keys, output_file)
                    if txn_keys[0].lower() == 'i':
                        is_successfully_executed = process_i(session, txn_keys, output_file)
                    if txn_keys[0].lower() == 'o':
                        is_successfully_executed = process_o(session, txn_keys, output_file)
                    if txn_keys[0].lower() == 'd':
                        is_successfully_executed = process_d(session, txn_keys, output_file)
                        if not is_successfully_executed:
                            print("failed txn d")
                    if txn_keys[0].lower() == 'r':
                        is_successfully_executed = process_r(session, txn_keys, output_file)
                        if (not is_successfully_executed):
                            print("failed txn r")
                    if is_successfully_executed:
                        txn_end_time = time.time()
                        latency = (txn_end_time - txn_start_time) * 1000  # Convert to ms
                        latencies.append(latency)
                        total_transactions += 1
        cluster.shutdown()
    except Exception as e:
        print(f"An unexpected error occurred: {type(e).__name__}")
        print(str(e))
    finally:
        elapsed_time = time.time() - start_time  # In seconds
        throughput = total_transactions / elapsed_time  # Transactions per second

        avg_latency = statistics.mean(latencies)
        median_latency = statistics.median(latencies)
        perc_95_latency = statistics.quantiles(latencies, n=100)[94]  # 95th percentile
        perc_99_latency = statistics.quantiles(latencies, n=100)[98]  # 99th percentile

        
        # stderr for each client
        with open(f"{shared_dir}stderr_client{filename}", "w") as f:
            f.write(f"Total number of transactions processed: {total_transactions}\n")
            f.write(f"Total elapsed time for processing the transactions: {elapsed_time:.2f} seconds\n")
            f.write(f"Transaction throughput: {throughput:.2f} transactions/second\n")
            f.write(f"Average transaction latency: {avg_latency:.2f} ms\n")
            f.write(f"Median transaction latency: {median_latency:.2f} ms\n")
            f.write(f"95th percentile transaction latency: {perc_95_latency:.2f} ms\n")
            f.write(f"99th percentile transaction latency: {perc_99_latency:.2f} ms\n")

        # client.csv for all clients
        with open(f"{shared_dir}client.csv", 'a') as f:
            writer = csv.writer(f)
            if f.tell() == 0:
                writer.writerow(["client_number", "measurement_a","measurement_b","measurement_c","measurement_d","measurement_e","measurement_f","measurement_g"])
            writer.writerow([client, total_transactions, round(elapsed_time, 2), round(throughput,2), round(avg_latency,2), round(median_latency,2), round(perc_95_latency,2), round(perc_99_latency,2)])
        


