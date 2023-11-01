import psycopg2
from psycopg2 import sql

# parameters link to citus 
host="localhost"
database="project"
user="cs4224d"
password="1234"

conn = None

def get_database_statistics(host, database, user, password):
    try:
        # Establish connection to the database
        conn = psycopg2.connect(host=host, database=database, user=user, password=password)
        cur = conn.cursor()
        # print('connection success to:')
        # print(f'host: {host}  database:{database}  user:{user}')

        # i. Sum of W YTD from Warehouse
        cur.execute("SELECT SUM(w_ytd) FROM warehouse")
        result_i = cur.fetchone()[0]

        # ii. Sum of D YTD and Sum of D NEXT O ID from District
        cur.execute("SELECT SUM(d_ytd), SUM(d_next_o_id) FROM district")
        result_ii = cur.fetchone()

        # iii. Sum of C BALANCE, Sum of C YTD PAYMENT, Sum of C PAYMENT CNT, Sum of C DELIVERY CNT from Customer
        cur.execute("SELECT SUM(c_balance), SUM(c_ytd_payment), SUM(c_payment_cnt), SUM(c_delivery_cnt) FROM customer")
        result_iii = cur.fetchone()

        # iv. Max of O ID and Sum of O OL CNT from Order
        cur.execute("SELECT MAX(o_id), SUM(o_ol_cnt) FROM orders")
        result_iv = cur.fetchone()

        # v. Sum of OL AMOUNT and Sum of OL QUANTITY from Order-Line
        cur.execute("SELECT SUM(ol_amount), SUM(ol_quantity) FROM order_line")
        result_v = cur.fetchone()

        # vi. Sum of S QUANTITY, Sum of S YTD, Sum of S ORDER CNT, Sum of S REMOTE CNT from Stock
        cur.execute("SELECT SUM(s_quantity), SUM(s_ytd), SUM(s_order_cnt), SUM(s_remote_cnt) FROM stock")
        result_vi = cur.fetchone()

        conn.commit()
        return result_i, result_ii, result_iii, result_iv, result_v, result_vi


    except psycopg2.DatabaseError as e:
        # If an error occurs, rollback the transaction
        conn.rollback()
        print(f"Error: {e}")
        return None

    finally:
        # Close the cursor and connection
        if conn:
            conn.close()

        if cur: 
            cur.close()

statistics = get_database_statistics(host, database, user, password)

if statistics:
    # 定义统计信息的标题和内容
    data = [
        ["Sum of W YTD", statistics[0]],
        ["Sum of D YTD", statistics[1][0]],
        ["Sum of D NEXT O ID", statistics[1][1]],
        ["Sum of C BALANCE", statistics[2][0]],
        ["Sum of C YTD PAYMENT", statistics[2][1]],
        ["Sum of C PAYMENT CNT", statistics[2][2]],
        ["Sum of C DELIVERY CNT", statistics[2][3]],
        ["Max of O ID", statistics[3][0]],
        ["Sum of O OL CNT", statistics[3][1]],
        ["Sum of OL AMOUNT", statistics[4][0]],
        ["Sum of OL QUANTITY", statistics[4][1]],
        ["Sum of S QUANTITY", statistics[5][0]],
        ["Sum of S YTD", statistics[5][1]],
        ["Sum of S ORDER CNT", statistics[5][2]],
        ["Sum of S REMOTE CNT", statistics[5][3]]
    ]
    
    for item in data:
        print(f"{item[0]}: {item[1]}")
    # # 将统计信息写入CSV文件
    # with open(csv_file, mode='w', newline='') as file:
    #     writer = csv.writer(file)
    #     writer.writerows(data)

    # print(f"Statistics saved to {csv_file}.")
else:
    print("Failed to retrieve statistics.")


