import psycopg2
from psycopg2 import sql

# parameters link to citus 
host="localhost"
database="project"
user="cs4224d"
password="1234"

# connect to citus
conn = psycopg2.connect(host=host, database=database, user=user, password=password)
cur = conn.cursor()
# print('connection success to:')
# print(f'host: {host}  database:{database}  user:{user}')

try: 
    data_modeling = sql.SQL("""
        -- import warehouse data
        COPY warehouse (w_id, w_name, w_street1, w_street2, w_city, w_state, w_zip, w_tax, w_ytd) 
        FROM '/temp/teamd-data/project_files/data_files/warehouse.csv' DELIMITER ',';

        -- import district data
        COPY district (d_w_id, d_id, d_name, d_street1, d_street2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id)
        FROM '/temp/teamd-data/project_files/data_files/district.csv' DELIMITER ',';

        -- Import customer data
        COPY customer (c_w_id, c_d_id, c_id, c_first, c_middle, c_last, c_street1, c_street2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data)
        FROM '/temp/teamd-data/project_files/data_files/customer.csv' DELIMITER ',';

        -- Import orders data
        COPY order (o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d)
        FROM '/temp/teamd-data/project_files/data_files/order.csv' DELIMITER ',' CSV NULL 'null';

        -- Import item data
        COPY item (i_id, i_name, i_price, i_im_id, i_data)
        FROM '/temp/teamd-data/project_files/data_files/item.csv' DELIMITER ',';

        -- Import order_line data
        COPY order_line (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info)
        FROM '/temp/teamd-data/project_files/data_files/order-line.csv' DELIMITER ',' CSV NULL 'null';

        -- Import stock data
        COPY stock (s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_data)
        FROM '/temp/teamd-data/project_files/data_files/stock.csv' DELIMITER ',';
    """)

    cur.execute(data_modeling)
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