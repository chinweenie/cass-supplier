import os
import pandas as pd
import time
import statistics
import sys

from cassandra.cluster import Cluster
from decimal import Decimal
from cassandra.query import BatchStatement, SimpleStatement
import pandas as pd

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
    # print(formatted_res)
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

    user_lookup_statement = SimpleStatement("SELECT * FROM customers WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s")
    user_res = db.execute(user_lookup_statement, (c_w_id, c_d_id, c_id)).one()
    if not user_res:
        output_file.write(f"customer not found")
        return executed

    warehouses = SimpleStatement(f"""SELECT * FROM warehouses WHERE w_id = %s""")
    warehouses_res = db.execute(warehouses, (c_w_id,)).one()
    if not warehouses_res:
        output_file.write(f"warehouse not found")
        return executed

    districts = SimpleStatement(f"""SELECT * FROM districts WHERE d_w_id = %s AND d_id = %s""")
    districts_res = db.execute(districts, (c_w_id, c_d_id)).one()
    if not districts_res:
        output_file.write(f"district not found")
        return executed

    old_balance = Decimal(user_res.c_balance)
    new_balance = old_balance - payment

    new_ytd_balance = user_res.c_ytd_payment + float(payment)
    new_payment_cnt = user_res.c_payment_cnt + 1

    try:
        batch = BatchStatement()
        update_customer_statement = db.prepare(
            "UPDATE customers SET c_balance = ?, c_ytd_payment = ?, c_payment_cnt = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?")
        batch.add(update_customer_statement, (new_balance, new_ytd_balance, new_payment_cnt, c_w_id, c_d_id, c_id))

        delete_query = db.prepare(
            "DELETE FROM top_balances WHERE c_w_id = ? AND c_balance = ? AND c_d_id = ? AND c_id = ? AND dummy_partition_key = 'global'")
        batch.add(delete_query, (c_w_id, old_balance, c_d_id, c_id))

        insert_query = db.prepare(
            "INSERT INTO top_balances (c_balance, c_w_id, c_id, c_d_id, dummy_partition_key, c_name, w_name, d_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
        batch.add(insert_query, (
            new_balance, c_w_id, c_id, c_d_id, 'global', user_res.c_name, warehouses_res.w_name,
            districts_res.d_name))

        db.execute(batch)

    except Exception as e:
        output_file.write(f"Error occurred: {e}")
        return executed

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
    statement = SimpleStatement(f"""SELECT c_name, c_balance, w_name, d_name FROM top_balances LIMIT 10;""")
    res = db.execute(statement)
    formatted_res = format_res(res)
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

    last_order_num_lookup_statement = db.prepare("SELECT * FROM districts WHERE d_w_id = ? AND d_id = ?")
    district_res = db.execute(last_order_num_lookup_statement, (w_id, d_id))
    last_order_num = district_res[0].d_next_o_id

    items_lookup_statement = db.prepare(
        'select s_quantity, i_id from stock_level_transaction where w_id = ? and d_id = ? and ol_o_id >= ?')
    items_res = db.execute(items_lookup_statement, (w_id, d_id, last_order_num - L))

    low_quantity_item_set = set()
    for row in items_res:
        if (row.s_quantity < T):
            low_quantity_item_set.add(row.i_id)

    print(len(low_quantity_item_set))
    output_file.write(str(len(low_quantity_item_set)))
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

    last_order_num_lookup_statement = db.prepare("SELECT * FROM districts WHERE d_w_id = ? AND d_id = ?")
    N_res = db.execute(last_order_num_lookup_statement, (w_id, d_id))
    N = N_res[0].d_next_o_id

    last_L_order_lookup_statement = db.prepare(
        'select o_id, o_entry_d, c_name, i_id, i_name, ol_quantity from popular_item_transaction where w_id=? and d_id=? and o_id >= ?;')
    S_res = db.execute(last_L_order_lookup_statement, (w_id, d_id, N - L))
    S_df = pd.DataFrame(list(S_res))

    s_without_id_df = S_df.drop(['i_id'], axis=1)

    p_df = S_df[['i_id', 'i_name']]
    p_df = p_df.value_counts().reset_index().rename(columns={"count": "percentage"})
    p_df['percentage'] = (p_df['percentage'] * 100 / L).round(2)
    p_df = p_df.drop(['i_id'], axis=1)

    formatted_res = format_res({"W_ID": w_id, "D_ID": d_id, "L": L},
                               s_without_id_df,
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

    if len(oldest_undelivered_orders) < 10:
        return executed

    update_carrier_stmt = db.prepare("UPDATE orders SET o_carrier_id = ? WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?")
    update_orders_by_customer_stt = db.prepare("UPDATE orders_by_customer SET o_carrier_id = ? WHERE c_w_id = ? AND "
                                               "c_d_id = ? AND c_id = ? AND o_id = ? ")

    get_order_lines_stmt = db.prepare("SELECT * FROM order_lines WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?")
    update_order_lines_stmt = db.prepare("""UPDATE order_lines SET ol_delivery_d = ? WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? 
            AND ol_number = ?""")
    get_customer_row_values = db.prepare(
        "SELECT c_balance, c_delivery_cnt, c_w_id, c_d_id, c_id, c_name FROM customers WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?")
    update_customer_stmt = db.prepare(
        "UPDATE customers SET c_balance = ?, c_delivery_cnt = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?")

    # update carrier_id in orders for each delivered order
    # update all order lines for the order to current date and time
    # update customer balance by total price of the orders
    # update customer delivery count by 1
    for d_id in range(1, 11):
        order = oldest_undelivered_orders[d_id - 1]

        if (order == None):
            print(f"warehouse: {w_id} district: {d_id} has no undelivered orders!")
            continue

        o_w_id = int(order.o_w_id)
        o_d_id = int(order.o_d_id)
        o_id = int(order.o_id)
        o_c_id = int(order.o_c_id)

        # update carrier_id in orders table
        db.execute(update_carrier_stmt, [carrier_id, o_w_id, o_d_id, o_id])
        # update carrier_id in orders_by_customer table
        db.execute(update_orders_by_customer_stt, (carrier_id, o_w_id, o_d_id, o_c_id, o_id))

        # get order_lines corresponding to the order
        order_lines = db.execute(get_order_lines_stmt, [o_w_id, o_d_id, o_id])

        # print("w_id: " + str(o_w_id) + " d_id: " + str(o_d_id) + " c_id: " + str(o_c_id))
        # get customer balance and delivery count values from table. searched by unique key so only 1 result
        customer_values = db.execute(get_customer_row_values, [o_w_id, o_d_id, o_c_id]).one()

        # customer not found
        if (customer_values == None):
            return executed

        # update all order_lines to current date and time
        curr_timestamp = datetime.now()
        total_order_amount = Decimal(customer_values.c_balance)
        new_delivery_count = int(customer_values.c_delivery_cnt) + 1
        customer_old_balance = Decimal(customer_values.c_balance)
        for ol in order_lines:
            ol_number = ol.ol_number

            # update order delivery date time
            db.execute(update_order_lines_stmt, [curr_timestamp, o_w_id, o_d_id, o_id, ol_number])

            # calculate total sum of order
            total_order_amount = total_order_amount + Decimal(ol.ol_amount)

        # update customer tables with total amounts and delivery count
        db.execute(update_customer_stmt, [total_order_amount, new_delivery_count, o_w_id, o_d_id, o_c_id])

        # Handle top_balances to update c_balance
        warehouses = SimpleStatement(f"""SELECT * FROM warehouses WHERE w_id=%s""")
        warehouses_res = db.execute(warehouses, (customer_values.c_w_id,)).one()
        districts = SimpleStatement(
            f"""SELECT * FROM districts WHERE d_w_id=%s AND d_id=%s""")
        districts_res = db.execute(districts, (customer_values.c_w_id, customer_values.c_d_id)).one()
        if not districts_res:
            output_file.write("district not found")
            return executed
        if not warehouses_res:
            output_file.write("warehouse not found")
            return executed

        if customer_old_balance != Decimal(total_order_amount):
            batch = BatchStatement()
            delete_query = db.prepare(
                "DELETE FROM top_balances WHERE c_w_id = ? AND c_balance = ? AND c_d_id = ? AND c_id = ? AND "
                "dummy_partition_key = 'global'")
            batch.add(delete_query,
                      (customer_values.c_w_id, customer_old_balance, customer_values.c_d_id, customer_values.c_id))
            insert_query = db.prepare(
                "INSERT INTO top_balances (c_balance, c_w_id, c_id, c_d_id, dummy_partition_key, c_name, w_name, "
                "d_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")

            batch.add(insert_query, (
                Decimal(total_order_amount), customer_values.c_w_id, customer_values.c_id, customer_values.c_d_id,
                'global',
                customer_values.c_name, warehouses_res.w_name,
                districts_res.d_name))
            db.execute(batch)

    delete_order_stmt = db.prepare("""DELETE FROM undelivered_orders_by_warehouse_district 
            WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?""")
    # delete delivered orders from undelivered orders table
    for order in oldest_undelivered_orders:
        if (order == None):
            continue
        db.execute(delete_order_stmt, [order.o_w_id, order.o_d_id, order.o_id])

    executed = True
    return executed


# Finds related customers
def process_r(db, values, output_file):
    executed = False
    w_id = int(values[1])
    d_id = int(values[2])
    c_id = int(values[3])

    get_items_stmt = db.prepare(
        "SELECT * FROM orders_by_warehouse_district_customer WHERE w_id = ? AND d_id = ? AND c_id = ?")
    items_table = db.execute(get_items_stmt, [w_id, d_id, c_id])

    executed = True

    # work with df from now, since we'll be joining
    items_df = pd.DataFrame(items_table)
    temp_items_df = items_df.rename(columns={'i_id': 'i2_id'})

    items_df = items_df.rename(columns={'i_id': 'i1_id'})

    # join 2 items table to get a table with 2 items per row given an order
    two_items_df = items_df.merge(temp_items_df, on=['w_id', 'd_id', 'c_id', 'o_id', 'ol_number'], how='inner')

    # filter rows with 2 of the same items
    two_items_df = two_items_df[two_items_df['i1_id'] != two_items_df['i2_id']]

    # join 2 items table to get a table with 2 different customers with the same items in their order`
    temp_2_items_df = two_items_df.rename(columns={'w_id': 'w2_id', 'd_id': 'd2_id',
                                                   'c_id': 'c2_id', 'o_id': 'o2_id', 'ol_number': 'ol_number2'})

    two_items_df = two_items_df.rename(columns={'w_id': 'w1_id', 'd_id': 'd1_id',
                                                'c_id': 'c1_id', 'o_id': 'o1_id', 'ol_number': 'ol_number1'})

    r_c_i = two_items_df.merge(temp_2_items_df, on=['i1_id', 'i2_id'], how='inner')

    # filter rows where customers are the same
    related_customers = r_c_i[r_c_i['w1_id'] != r_c_i['w2_id']]

    # write customer identifier
    output_file.write(f"C_W_ID: {w_id} C_D_ID: {d_id} C_ID: {c_id}")

    formatted_res = format_res(related_customers)
    output_file.write(formatted_res)

    return executed


if __name__ == '__main__':
    print(sys.argv)
    if len(sys.argv) != 6:
        print("You must provide exactly 5 arguments!")
        sys.exit(1)

    ip_address = sys.argv[1]
    filenames = [sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]]

    print(f"Received IP Address: {ip_address}")
    print(f"Received filename: {filenames}")

    cluster = Cluster([ip_address])
    session = cluster.connect()
    session.set_keyspace('supplier')
    session.default_timeout = 120
    directory = "/temp/teamd-cass/apache-cassandra-4.1.3/bin/xact_files/"
    # directory = ''
    total_transactions = 0
    latencies = []  # List to store latency of each transaction
    start_time = time.time()
    try:
        count = 0
        while count < len(filenames):
            with open(f"{directory}stdout", 'w') as output_file:
                dir_filename = os.path.join(directory, filenames[count])
                with open(dir_filename, 'r') as file:
                    for line in file:
                        print(line.strip())
                        txn_start_time = time.time()

                        txn_keys = line.strip().split(',')
                        is_successfully_executed = False
                        # if txn_keys[0].lower() == 'N':
                        # handle m more lines
                        # is_successfully_executed = process_n(session, txn_keys, output_file)
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
            count += 1

        cluster.shutdown()
        elapsed_time = time.time() - start_time  # In seconds
        throughput = total_transactions / elapsed_time  # Transactions per second

        avg_latency = statistics.mean(latencies)
        median_latency = statistics.median(latencies)
        perc_95_latency = statistics.quantiles(latencies, n=100)[94]  # 95th percentile
        perc_99_latency = statistics.quantiles(latencies, n=100)[98]  # 99th percentile

        with open(f"{directory}stderr", "w") as f:
            f.write(f"Total number of transactions processed: {total_transactions}\n")
            f.write(f"Total elapsed time for processing the transactions: {elapsed_time:.2f} seconds\n")
            f.write(f"Transaction throughput: {throughput:.2f} transactions/second\n")
            f.write(f"Average transaction latency: {avg_latency:.2f} ms\n")
            f.write(f"Median transaction latency: {median_latency:.2f} ms\n")
            f.write(f"95th percentile transaction latency: {perc_95_latency:.2f} ms\n")
            f.write(f"99th percentile transaction latency: {perc_99_latency:.2f} ms\n")

    except Exception as e:
        print(f"An unexpected error occurred: {type(e).__name__}")
        print(str(e))
