import pandas as pd
import time
import statistics
import sys


from cassandra.cluster import Cluster
from decimal import Decimal
from cassandra.query import BatchStatement, SimpleStatement
from datetime import datetime

def format_res(*results_sets):
    output = []
    for results in results_sets:
        if isinstance(results, dict):  # Handle dictionary directly
            for key, value in results.items():
                output.append(f"{key}: {value}")
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
    res = db.execute(last_order_by_customer_statement, (c_w_id, c_d_id, c_id))
    if not res:
        return executed

    last_order_id = res[0].o_id
    last_order_items_statement = SimpleStatement(
        """SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_lines WHERE ol_w_id = %s AND ol_d_id = %s AND ol_o_id = %s""")
    order_items_res = db.execute(last_order_items_statement, (c_w_id, c_d_id, last_order_id))
    if not order_items_res:
        return executed
    formatted_res = format_res({"o_id": last_order_id, "o_entry_d": res[0].o_entry_d, "o_carrier_id": res[0].o_carrier_id}, order_items_res)
    # print(formatted_res)
    output_file.write(formatted_res)

    executed = True
    return executed


def process_p(db, values, output_file):
    executed = False
    if len(values) < 5:
        print("Not enough arguments of P txn!")
        return executed
    c_w_id = int(values[1])
    c_d_id = int(values[2])
    c_id = int(values[3])
    payment = Decimal(values[4])

    user_lookup_statement = SimpleStatement("SELECT * FROM customers WHERE c_w_id = %s AND c_id = %s")
    user_res = db.execute(user_lookup_statement, (c_w_id, c_id))
    if not user_res:
        return executed

    warehouses = SimpleStatement(f"""SELECT * FROM warehouses WHERE w_id={c_w_id}""")
    warehouses_res = db.execute(warehouses)
    if not warehouses_res:
        return executed

    districts = SimpleStatement(f"""SELECT * FROM districts WHERE d_w_id={c_w_id} AND d_id={c_d_id}""")
    districts_res = db.execute(districts)
    if not districts_res:
        return executed

    old_balance = user_res[0].c_balance
    new_balance = old_balance - payment
    new_ytd_balance = user_res[0].c_ytd_payment + float(payment)
    new_payment_cnt = user_res[0].c_payment_cnt + 1

    try:
        update_customer_statement = db.prepare(
            "UPDATE customers SET c_balance = ?, c_ytd_payment = ?, c_payment_cnt = ? WHERE c_w_id = ? AND c_id = ?")
        bound_statement = update_customer_statement.bind((new_balance, new_ytd_balance, new_payment_cnt, c_w_id, c_id))
        db.execute(bound_statement)

        batch = BatchStatement()
        delete_query = db.prepare(
            "DELETE FROM top_balances WHERE c_w_id = ? AND c_balance = ? AND c_id = ? AND dummy_partition_key = 'global'")
        batch.add(delete_query, (c_w_id, old_balance, c_id))
        insert_query = db.prepare(
            "INSERT INTO top_balances (c_balance, c_w_id, c_id, c_d_id, dummy_partition_key, c_name, w_name, d_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
        batch.add(insert_query, (
        new_balance, c_w_id, c_id, c_d_id, 'global', user_res[0].c_name, warehouses_res[0].w_name,
        districts_res[0].d_name))
        db.execute(batch)
    except:
        return executed

    latest_customer = SimpleStatement(f"""SELECT * FROM customers WHERE c_w_id={c_w_id} AND c_id={c_id}""")
    latest_customer_res = db.execute(latest_customer)

    formatted_res = format_res(latest_customer_res, warehouses_res, districts_res, {'payment': payment})
    # print(formatted_res)
    output_file.write(formatted_res)

    executed = True
    return executed


def process_t(db, values, output_file):
    executed = False
    statement = SimpleStatement(f"""SELECT c_name, c_balance, w_name, d_name FROM top_balances LIMIT 10;""")
    res = db.execute(statement)
    formatted_res = format_res(res)
    # print(formatted_res)
    output_file.write(formatted_res)
    executed = True
    return executed


def process_s(db, values):
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
        oldest_undelivered_order = db.execute(oldest_undel_order_per_wid_did, [w_id, d_id])
        oldest_undelivered_orders.append(oldest_undelivered_order)

    if (len(oldest_undelivered_orders) < 10):
        return executed


    update_carrier_stmt = db.prepare("UPDATE orders SET o_carrier_id = ? WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?")
    get_order_lines_stmt = db.prepare("SELECT * FROM order_lines WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?")
    update_order_lines_stmt = db.prepare("""UPDATE order_lines SET ol_delivery_d = ? WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? 
        AND ol_number = ?""")
    update_customer_stmt = db.prepare("""UPDATE customers SET c_balance = c_balance + ?, c_delivery_cnt = c_delivery_cnt + 1
        WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?""")


    # update carrier_id in orders for each delivered order
    # update all order lines for the order to current date and time
    # update customer balance by total price of the orders
    # update customer delivery count by 1
    for order in oldest_undelivered_orders:
        o_w_id = order.o_w_id
        o_d_id = order.o_d_id
        o_id = order.o_id
        o_c_id = order.o_c_id
        
        # update carrier_id in orders table
        db.execute(update_carrier_stmt, [carrier_id, o_w_id, o_d_id,o_id])

        # get order_lines corresponding to the order
        order_lines = db.execute(get_order_lines_stmt, [o_w_id, o_d_id, o_id])

        # update all order_lines to current date and time
        curr_timestamp = datetime.now()
        total_order_amount = 0

        for ol in order_lines:
            ol_number = ol.ol_number

            # update order delivery date time
            db.execute(update_order_lines_stmt, [curr_timestamp, o_w_id, o_d_id, o_id, ol_number])

            # calculate total sum of order
            total_order_amount = total_order_amount + float(ol.ol_amount)

            # update customer tables with total amounts and delivery count
            db.execute(update_customer_stmt, [total_order_amount, o_w_id, o_d_id, o_c_id])


    delete_order_stmt = db.prepare("""DELETE FROM undelivered_orders_by_warehouse_district 
        WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?""")
    # delete delivered orders from undelivered orders table
    for order in oldest_undelivered_orders:
        db.execute(delete_order_stmt, [order.o_w_id, order.o_d_id, order.o_id])

    executed = True
    return executed
            

# Finds related customers
def process_ r(db, values, output_file):
    executed = False
    w_id = int(values[1])
    d_id = int(values[2])
    c_id = int(values[3])

    get_items_stmt = db.prepare("SELECT * FROM orders_by_warehouse_district_customer WHERE w_id = ? AND d_id = ? AND c_id = ?")
    items_table = db.execute(get_items_stmt, [w_id, d_id, c_id])

    executed = True
    
    # work with df from now, since we'll be joining
    items_df = pd.DataFrame(items_table)
    temp_items_df = items_df.rename(columns={'i_id': 'i2_id'})

    items_df = items_df.rename(columns={'i_id': 'i1_id'})

    # join 2 items table to get a table with 2 items per row given an order
    2_items_df = items_df.merge(temp_items_df, on=['w_id', 'd_id', 'c_id', 'o_id', 'ol_id'], how='inner')

    # filter rows with 2 of the same items
    2_items_df = 2_items_df[2_items_df['i1_id'] != 2_items_df['i2_id']]

    # join 2 items table to get a table with 2 different customers with the same items in their order`
    temp_2_items_df = 2_items_df.rename(columns={'w_id': 'w2_id', 'd_id': 'd2_id',
        'c_id': 'c2_id', 'o_id': 'o2_id', 'ol_number': 'ol_number2'})

    2_items_df = 2_items_df.rename(columns={'w_id': 'w1_id', 'd_id': 'd1_id',
        'c_id': 'c1_id', 'o_id': 'o1_id', 'ol_number': 'ol_number1'})
    
    r_c_i = 2_items_df.merge(temp_2_items_df, on=['i1_id', 'i2_id'], how='inner')
    
    # filter rows where customers are the same
    related_customers = r_c_i[r_c_i['w1_id'] != r_c_i['w2_id']]

    # write customer identifier
    output_file.write(f"C_W_ID: {w_id} C_D_ID: {d_id} C_ID: {c_id}")

    formatted_res = format_res(related_customers)
    output_file.write(formattted_res)

    return executed


if __name__ == '__main__':
    print(sys.argv)
    if len(sys.argv) != 6:
        print("You must provide exactly 5 arguments!")
        sys.exit(1)

    ip_address = sys.argv[1]
    filenames = [sys.argv[2], sys.argv[3], sys.argv[4]]

    print(f"Received IP Address: {ip_address}")
    print(f"Received filename: {filenames}")
    cluster = Cluster([ip_address])
    session = cluster.connect()
    session.set_keyspace('supplier')

    total_transactions = 0
    latencies = []  # List to store latency of each transaction
    start_time = time.time()

    try:
        count = 0
        while count < len(filenames):
            with open('stdout', 'w') as output_file:
                with open(filenames[count], 'r') as file:
                    for line in file:
                        print(line.strip())
                        txn_start_time = time.time()

                        txn_keys = line.strip().split(',')
                        is_successfully_executed = False
                        if txn_keys[0].lower() == 'p':
                            is_successfully_executed = process_p(session, txn_keys, output_file)
                        if txn_keys[0].lower() == 't':
                            is_successfully_executed = process_t(session, txn_keys, output_file)
                        if txn_keys[0].lower() == 's':
                            is_successfully_executed = process_s(session, txn_keys)
                        if txn_keys[0].lower() == 'o':
                            is_successfully_executed = process_o(session, txn_keys, output_file)
                        if txn_keys[0].lower() == 'd':
                            is_successfully_executed = process_d(session, txn_keys, output_file)
                            if (!is_successfully_executed):
                                print("failed txn d")
                        if txn_keys[0].lower() == 'r':
                            is_successfully_executed = process_r(session, txn_keys, output_file)
                            if (!is_successfully_executed):
                                print("failed txn r")
                        if is_successfully_executed:
                            txn_end_time = time.time()
                            latency = (txn_end_time - txn_start_time) * 1000  # Convert to ms
                            latencies.append(latency)
                            total_transactions += 1
            count += 1
    except FileNotFoundError:
        print(f"File  not found!")

    cluster.shutdown()
    elapsed_time = time.time() - start_time  # In seconds
    throughput = total_transactions / elapsed_time  # Transactions per second

    avg_latency = statistics.mean(latencies)
    median_latency = statistics.median(latencies)
    perc_95_latency = statistics.quantiles(latencies, n=100)[94]  # 95th percentile
    perc_99_latency = statistics.quantiles(latencies, n=100)[98]  # 99th percentile

    with open("stderr", "w") as f:
        f.write(f"Total number of transactions processed: {total_transactions}\n")
        f.write(f"Total elapsed time for processing the transactions: {elapsed_time:.2f} seconds\n")
        f.write(f"Transaction throughput: {throughput:.2f} transactions/second\n")
        f.write(f"Average transaction latency: {avg_latency:.2f} ms\n")
        f.write(f"Median transaction latency: {median_latency:.2f} ms\n")
        f.write(f"95th percentile transaction latency: {perc_95_latency:.2f} ms\n")
        f.write(f"99th percentile transaction latency: {perc_99_latency:.2f} ms\n")
