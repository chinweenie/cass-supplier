COPY supplier.warehouses (W_ID,W_NAME,W_TAX,W_YTD,W_ADDRESS) FROM '/temp/teamd-cass/apache-cassandra-4.1.3/bin/data_files/warehouse_df.csv' WITH DELIMITER=',' AND HEADER=TRUE;
COPY supplier.districts (d_w_id,d_id,d_name,d_tax,d_ytd,d_next_o_id,d_address) FROM '/temp/teamd-cass/apache-cassandra-4.1.3/bin/data_files/district_df.csv' WITH DELIMITER=',' AND HEADER=TRUE;
COPY supplier.items (i_id,i_name,i_price,i_im_id,i_data) FROM '/temp/teamd-cass/apache-cassandra-4.1.3/bin/data_files/item_df.csv' WITH DELIMITER=',' AND HEADER=TRUE;
COPY supplier.orders (o_w_id,o_d_id,o_id,o_c_id,o_ol_cnt,o_carrier_id,o_all_local,o_entry_d) FROM '/temp/teamd-cass/apache-cassandra-4.1.3/bin/data_files/order_df.csv' WITH DELIMITER=',' AND HEADER=TRUE;
COPY supplier.order_lines (ol_w_id,ol_d_id,ol_o_id,ol_number,ol_i_id,ol_delivery_d,ol_amount,ol_supply_w_id,ol_quantity,ol_dist_info) FROM '/temp/teamd-cass/apache-cassandra-4.1.3/bin/data_files/order_line_df.csv' WITH DELIMITER=',' AND HEADER=TRUE;
COPY supplier.stocks (s_w_id,s_i_id,s_quantity,s_ytd,s_order_cnt,s_remote_cnt,s_dist_01,s_dist_02,s_dist_03,s_dist_04,s_dist_05,s_dist_06,s_dist_07,s_dist_08,s_dist_09,s_dist_10,s_data) FROM '/temp/teamd-cass/apache-cassandra-4.1.3/bin/data_files/stock_df.csv' WITH DELIMITER=',' AND HEADER=TRUE;
COPY supplier.customers (c_w_id,c_d_id,c_id,c_phone,c_since,c_credit,c_credit_lim,c_discount,c_balance,c_ytd_payment,c_payment_cnt,c_delivery_cnt,c_data,c_address,c_name,c_d_name,c_w_name) FROM '/temp/teamd-cass/apache-cassandra-4.1.3/bin/data_files/customer_df.csv' WITH DELIMITER=',' AND HEADER=TRUE;
COPY supplier.orders_by_customer (c_w_id,c_d_id,c_id,o_id,o_carrier_id,o_entry_d) FROM '/temp/teamd-cass/apache-cassandra-4.1.3/bin/data_files/orders_by_customer_df.csv' WITH DELIMITER=',' AND HEADER=TRUE;
COPY supplier.undelivered_orders_by_warehouse_district (o_w_id,o_d_id,o_id,o_c_id,o_carrier_id) FROM '/temp/teamd-cass/apache-cassandra-4.1.3/bin/data_files/undelivered_orders.csv' WITH DELIMITER=',' AND HEADER=TRUE;
COPY supplier.orders_by_warehouse_district_customer (w_id,d_id,c_id,o_id,ol_number,i_id) FROM '/temp/teamd-cass/apache-cassandra-4.1.3/bin/data_files/orders_by_warehouse_district_customer_df.csv' WITH DELIMITER=',' AND HEADER=TRUE;
COPY supplier.popular_item_transaction (w_id,d_id,o_id,o_entry_d,c_name,ol_number,i_id,ol_quantity,i_name) FROM '/temp/teamd-cass/apache-cassandra-4.1.3/bin/data_files/popular_items_table.csv' WITH DELIMITER=',' AND HEADER=TRUE;
-- COPY supplier.top_balances (c_id,c_w_id,c_d_id,c_balance,c_name,c_d_name,c_w_name) FROM '/temp/teamd-cass/apache-cassandra-4.1.3/bin/data_files/top_balances_df.csv' WITH DELIMITER=',' AND HEADER=TRUE;


