import time
import statistics
from collections import namedtuple

from cassandra.cluster import Cluster
import sys
from decimal import Decimal
from cassandra.query import BatchStatement, SimpleStatement

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


def process_p(db, values, output_file):
    executed = False
    if len(values) < 5:
        print("Not enough arguments of P txn!")
        return executed
    c_w_id = int(values[1])
    c_d_id = int(values[2])
    c_id = int(values[3])
    payment = Decimal(values[4])

    user_lookup_statement = db.prepare("SELECT * FROM customers WHERE c_w_id = ? AND c_id = ?")
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
        batch.add(insert_query, (new_balance, c_w_id, c_id, c_d_id, 'global', user_res[0].c_name, warehouses_res[0].w_name ,districts_res[0].d_name))
        db.execute(batch)
    except:
        return executed

    latest_customer = SimpleStatement(f"""SELECT * FROM customers WHERE c_w_id={c_w_id} AND c_id={c_id}""")
    latest_customer_res = db.execute(latest_customer)

    formatted_res = format_res(latest_customer_res, warehouses_res, districts_res, {'payment': payment})
    print(formatted_res)
    output_file.write(formatted_res)

    executed = True
    return executed


def process_t(db, values, output_file):
    executed = False
    statement = SimpleStatement(f"""SELECT c_name, c_balance, w_name, d_name FROM top_balances LIMIT 10;""")
    res = db.execute(statement)
    formatted_res = format_res(res)
    print(formatted_res)
    output_file.write(formatted_res)
    executed = True
    return executed


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("You must provide exactly 2 arguments!")
        sys.exit(1)

    ip_address = sys.argv[1]
    filename = sys.argv[2]
    print(f"Received IP Address: {ip_address}")
    print(f"Received filename: {filename}")
    cluster = Cluster([ip_address])
    session = cluster.connect()
    session.set_keyspace('supplier')

    total_transactions = 0
    latencies = []  # List to store latency of each transaction
    start_time = time.time()

    try:
        with open(filename, 'r') as file:
            with open('stdout', 'w') as output_file:
                for line in file:
                    print(line.strip())
                    txn_start_time = time.time()
                    txn_keys = line.strip().split(',')
                    if txn_keys[0].lower() == 'p':
                        is_successfully_executed = process_p(session, txn_keys, output_file)
                    if txn_keys[0].lower() == 't':
                        is_successfully_executed = process_t(session, txn_keys, output_file)
                    if is_successfully_executed:
                        txn_end_time = time.time()
                        latency = (txn_end_time - txn_start_time) * 1000  # Convert to ms
                        latencies.append(latency)
                        total_transactions += 1
    except FileNotFoundError:
        print(f"File {filename} not found!")

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
