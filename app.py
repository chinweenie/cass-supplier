import os
import time
import statistics

from cassandra.cluster import Cluster
import sys
from decimal import Decimal
from cassandra.query import BatchStatement, SimpleStatement
import pandas as pd


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
    res = db.execute(last_order_by_customer_statement, (c_w_id, c_d_id, c_id))
    if not res:
        return executed

    last_order_id = res[0].o_id
    last_order_items_statement = SimpleStatement(
        """SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_lines WHERE ol_w_id = %s AND ol_d_id = %s AND ol_o_id = %s""")
    order_items_res = db.execute(last_order_items_statement, (c_w_id, c_d_id, last_order_id))
    if not order_items_res:
        return executed
    formatted_res = format_res(
        {"o_id": last_order_id, "o_entry_d": res[0].o_entry_d, "o_carrier_id": res[0].o_carrier_id}, order_items_res)
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

    user_lookup_statement = SimpleStatement("SELECT * FROM customers WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s")
    user_res = db.execute(user_lookup_statement, (c_w_id, c_d_id, c_id))
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
            "UPDATE customers SET c_balance = ?, c_ytd_payment = ?, c_payment_cnt = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?")
        bound_statement = update_customer_statement.bind(
            (new_balance, new_ytd_balance, new_payment_cnt, c_w_id, c_d_id, c_id))
        db.execute(bound_statement)

        batch = BatchStatement()
        delete_query = db.prepare(
            "DELETE FROM top_balances WHERE c_w_id = ? AND c_balance = ? AND c_d_id = ? AND c_id = ? AND dummy_partition_key = 'global'")
        batch.add(delete_query, (c_w_id, old_balance, c_d_id, c_id))
        insert_query = db.prepare(
            "INSERT INTO top_balances (c_balance, c_w_id, c_id, c_d_id, dummy_partition_key, c_name, w_name, d_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
        batch.add(insert_query, (
            new_balance, c_w_id, c_id, c_d_id, 'global', user_res[0].c_name, warehouses_res[0].w_name,
            districts_res[0].d_name))
        db.execute(batch)
    except:
        return executed

    latest_customer = SimpleStatement(
        f"""SELECT * FROM customers WHERE c_w_id={c_w_id} AND c_d_id={c_d_id} AND c_id={c_id}""")
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


if __name__ == '__main__':
    print(sys.argv)
    if len(sys.argv) != 6:
        print("You must provide exactly 5 arguments!")
        sys.exit(1)

    ip_address = sys.argv[1]
    filenames = [sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]]

    print(f"Received IP Address: {ip_address}")
    print(f"Received filename: {filenames}")
    print(os.getcwd())

    cluster = Cluster([ip_address])
    session = cluster.connect()
    session.set_keyspace('supplier')
    directory = "/temp/teamd-cass/apache-cassandra-4.1.3/bin/xact_files/"

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
                        # if txn_keys[0].lower() == 'i':
                            # is_successfully_executed = process_i(session, txn_keys, output_file)
                        if txn_keys[0].lower() == 'o':
                            is_successfully_executed = process_o(session, txn_keys, output_file)
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
