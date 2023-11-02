import csv
import psycopg2
from psycopg2 import sql

# parameters link to citus 
host="localhost"
database="project"
user="cs4224d"
password="1234"
csv_file = "../result/dbstate.csv"
conn = None

def get_database_statistics(host, database, user, password):
    try:
        # Establish connection to the database
        conn = psycopg2.connect(host=host, database=database, user=user, password=password)
        cur = conn.cursor()
        # print('connection success to:')
        # print(f'host: {host}  database:{database}  user:{user}')

        # warehouse table statistic 
        cur.execute("SELECT SUM(w_ytd) FROM warehouse")
        result_1 = cur.fetchone()[0]

        # district table statistic
        cur.execute("SELECT SUM(d_ytd), SUM(d_next_o_id) FROM district")
        result_2 = cur.fetchone()

        # customer table statistic 
        cur.execute("SELECT SUM(c_balance), SUM(c_ytd_payment), SUM(c_payment_cnt), SUM(c_delivery_cnt) FROM customer")
        result_3 = cur.fetchone()

        # order table statistic 
        cur.execute("SELECT MAX(o_id), SUM(o_ol_cnt) FROM orders")
        result_4 = cur.fetchone()

        # order_line table statistic 
        cur.execute("SELECT SUM(ol_amount), SUM(ol_quantity) FROM order_line")
        result_5 = cur.fetchone()

        # stock table statistic 
        cur.execute("SELECT SUM(s_quantity), SUM(s_ytd), SUM(s_order_cnt), SUM(s_remote_cnt) FROM stock")
        result_6 = cur.fetchone()

        conn.commit()
        return result_1, result_2, result_3, result_4, result_5, result_6


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
    # define statistic data 
    data = [
        ["Sum of W_YTD", statistics[0]],
        ["Sum of D_YTD", statistics[1][0]],
        ["Sum of D_NEXT_O_ID", statistics[1][1]],
        ["Sum of C_BALANCE", statistics[2][0]],
        ["Sum of C_YTD_PAYMENT", statistics[2][1]],
        ["Sum of C_PAYMENT_CNT", statistics[2][2]],
        ["Sum of C_DELIVERY_CNT", statistics[2][3]],
        ["Max of O_ID", statistics[3][0]],
        ["Sum of O_OL_CNT", statistics[3][1]],
        ["Sum of OL_AMOUNT", statistics[4][0]],
        ["Sum of OL_QUANTITY", statistics[4][1]],
        ["Sum of S_QUANTITY", statistics[5][0]],
        ["Sum of S_YTD", statistics[5][1]],
        ["Sum of S_ORDER_CNT", statistics[5][2]],
        ["Sum of S_REMOTE_CNT", statistics[5][3]]
    ]

    for item in data:
        print(f"{item[0]}: {item[1]}")

    with open(csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(data)

    print(f"Statistics saved to {csv_file}.")
else:
    print("Failed to retrieve statistics.")


