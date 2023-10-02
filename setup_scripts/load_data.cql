COPY supplier.warehouses (W_ID,W_NAME,W_TAX,W_YTD,W_ADDRESS) FROM 'warehouse_df.csv' WITH DELIMITER=',' AND HEADER=TRUE;
COPY supplier.districts (d_w_id,d_id,d_name,d_tax,d_ytd,d_next_o_id,d_address) FROM 'district_df.csv' WITH DELIMITER=',' AND HEADER=TRUE;
COPY supplier.items (i_id,i_name,i_price,i_im_id,i_data) FROM 'item_df.csv' WITH DELIMITER=',' AND HEADER=TRUE;
COPY supplier.order_lines (ol_w_id,ol_d_id,ol_o_id,ol_number,ol_i_id,ol_delivery_d,ol_amount,ol_supply_w_id,ol_quantity,ol_dist_info) FROM 'order_line_df.csv' WITH DELIMITER=',' AND HEADER=TRUE;
COPY supplier.stocks (s_w_id,s_i_id,s_quantity,s_ytd,s_order_cnt,s_remote_cnt,s_dist_01,s_dist_02,s_dist_03,s_dist_04,s_dist_05,s_dist_06,s_dist_07,s_dist_08,s_dist_09,s_dist_10,s_data) FROM 'stock_df.csv' WITH DELIMITER=',' AND HEADER=TRUE;
COPY supplier.customers (c_w_id,c_d_id,c_id,c_phone,c_since,c_credit,c_credit_lim,c_discount,c_balance,c_ytd_payment,c_payment_cnt,c_delivery_cnt,c_data,c_address,c_name) FROM 'customer_df.csv' WITH DELIMITER=',' AND HEADER=TRUE;
COPY supplier.top_balances (c_id,c_w_id,c_d_id,c_balance,dummy_partition_key) FROM 'top_balances_df.csv' WITH DELIMITER=',' AND HEADER=TRUE;
COPY supplier.orders_by_customer (c_w_id,c_d_id,c_id,o_id,o_carrier_id,o_entry_d) FROM 'orders_by_customer_df.csv' WITH DELIMITER=',' AND HEADER=TRUE;