from cassandra.cluster import Cluster
import sys
from decimal import Decimal


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
    print_res(user_res)
    old_balance = user_res[0].c_balance

    top_balance_lookup = db.prepare("SELECT * FROM top_balances where c_w_id = ? AND AND c_balance = ? AND c_id = ?")
    top_balance_res = db.execure(top_balance_lookup, (c_w_id, old_balance, c_id))
    print_res(top_balance_res)

    warehouse_lookup = db.prepare("SELECT * FROM warehouses WHERE w_id = ?")
    warehouse_res = db.execute(warehouse_lookup, (c_w_id,))
    print_res(warehouse_res)

    district_lookup = db.prepare("SELECT * FROM districts WHERE d_w_id = ? AND d_id = ?")
    district_res = db.execute(district_lookup, (c_w_id, c_d_id))
    print_res(district_res)


    new_balance = old_balance - payment
    new_ytd_balance = user_res[0].c_ytd_balance + payment
    new_payment_cnt = user_res[0].c_payment_cnt + 1

    update_customer_statement = "UPDATE customers SET c_balance = ?, c_ytd_balance = ?, c_payment_cnt = ? WHERE c_w_id = ? AND c_id = ?"
    db.execute(update_customer_statement, (new_balance, new_ytd_balance, new_payment_cnt
                                           , c_w_id, c_id))
    
    update_top_balance_statement = "UPDATE top_balances set c_balance = ? WHERE c_w_id = ? AND c_balance = ? AND c_id = ?"
    db.execute(update_top_balance_statement, (new_balance, c_w_id, old_balance, c_id))

    latest_top_balances_lookup = "SELECT * FROM top_balances LIMIT 10;"
    latest_top_balances_res = db.execute(latest_top_balances_lookup)
    print(f"Check for latest top balances!")
    print_res(latest_top_balances_res)



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

    # rows = session.execute('SELECT * FROM warehouses LIMIT 10;')
    # for row in rows:
    #     print(row)
    cluster.shutdown()
