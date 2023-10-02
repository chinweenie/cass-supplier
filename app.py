from cassandra.cluster import Cluster
import sys
from decimal import Decimal

from cassandra.query import BatchStatement, SimpleStatement


def print_res(results):
    for row in results:
        for column_name, value in row._asdict().items():
            print(f"{column_name}: {value}")
    print("----")


def process_p(db, values):
    if len(values) < 5:
        print("Not enough arguments of P txn!")
        return
    c_w_id = int(values[1])
    c_d_id = int(values[2])
    c_id = int(values[3])
    payment = Decimal(values[4])

    user_lookup_statement = db.prepare("SELECT * FROM customers WHERE c_w_id = ? AND c_id = ?")
    user_res = db.execute(user_lookup_statement, (c_w_id, c_id))
    old_balance = user_res[0].c_balance

    new_balance = old_balance - payment
    new_ytd_balance = user_res[0].c_ytd_payment + float(payment)
    new_payment_cnt = user_res[0].c_payment_cnt + 1

    print(f"Check new balance {new_balance}")
    print(f"Check new_ytd_balance {new_ytd_balance}")
    print(f"Check new_payment_cnt {new_payment_cnt}")

    update_customer_statement = db.prepare("UPDATE customers SET c_balance = ?, c_ytd_payment = ?, c_payment_cnt = ? WHERE c_w_id = ? AND c_id = ?")
    bound_statement = update_customer_statement.bind((new_balance, new_ytd_balance, new_payment_cnt, c_w_id, c_id))
    db.execute(bound_statement)

    batch = BatchStatement()
    delete_query = db.prepare("DELETE FROM top_balances WHERE c_w_id = ? AND c_balance = ? AND c_id = ? AND dummy_partition_key = 'global'")
    batch.add(delete_query, (c_w_id, old_balance, c_id))
    insert_query = db.prepare("INSERT INTO top_balances (c_balance, c_w_id, c_id, c_d_id, dummy_partition_key) VALUES (?, ?, ?, ?, ?)")
    batch.add(insert_query, (new_balance, c_w_id, c_id, c_d_id, 'global'))
    db.execute(batch)

    latest_top_balances = SimpleStatement(f"""SELECT * FROM top_balances WHERE c_w_id ={c_w_id} AND c_balance={new_balance} AND c_id={c_id} AND dummy_partition_key='global'""")
    res = db.execute(latest_top_balances)
    print_res(res)

    latest_customer = SimpleStatement(f"""SELECT * FROM customers WHERE c_w_id={c_w_id} AND c_id={c_id}""")
    res = db.execute(latest_customer)
    print_res(res)

    # latest_top_balances_lookup = "SELECT * FROM top_balances LIMIT 10;"
    # latest_top_balances_res = db.execute(latest_top_balances_lookup)
    # print(f"Check for latest top balances!")
    # print_res(latest_top_balances_res)



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

    try:
        with open(filename, 'r') as file:
            desired_line_index = 4
            current_index = 0

            for line in file:
                if current_index == desired_line_index:
                    print(line)
                    break
                current_index += 1
                print(line.strip())
                txn_keys = line.strip().split(',')
                if txn_keys[0].lower() == 'p':
                    process_p(session, txn_keys)
    except FileNotFoundError:
        print(f"File {filename} not found!")

    cluster.shutdown()
