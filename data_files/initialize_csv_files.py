import json

import pandas

def merge_name(row):
    return {
        'first': row['c_first'],
        'middle': row['c_middle'],
        'last': row['c_last']
    }


def merge_cells(row):
    address_obj = {
        "street_1": row['street_1'],
        "street_2": row['street_2'],
        "city": row['city'],
        "state": row['state'],
        "zip": row['zip'],
    }
    return address_obj


def process_warehouse():
    warehouse_df = pandas.read_csv('warehouse.csv', dtype=str, header=None)
    warehouse_df.columns = ['w_id', 'w_name', 'street_1', 'street_2', 'city', 'state', 'zip', 'w_tax',
                            'w_ytd']
    warehouse_df['w_address'] = warehouse_df.apply(merge_cells, axis=1)
    warehouse_df = warehouse_df.drop(['street_1', 'street_2', 'city', 'state', 'zip'], axis=1)
    warehouse_df.to_csv('warehouse_df.csv',index=False, sep=',')


def process_district():
    district_df = pandas.read_csv('district.csv',header=None)
    district_df.columns = ['d_w_id', 'd_id', 'd_name', 'street_1', 'street_2', 'city', 'state', 'zip',
                           'd_tax', 'd_ytd', 'd_next_o_id']
    district_df['address'] = district_df.apply(merge_cells, axis=1)
    district_df = district_df.drop(['street_1', 'street_2', 'city', 'state', 'zip'], axis=1)
    district_df.to_csv('district_df.csv',index=False)

def process_items():
    item_df = pandas.read_csv('item.csv', header=None)
    item_df.columns = ['i_id', 'i_name', 'i_price', 'i_im_id', 'i_data']
    item_df.to_csv('item_df.csv',index=False)

def process_order():
    order_df = pandas.read_csv('order.csv', header=None)
    order_df.columns = ['o_w_id', 'o_d_id', 'o_id', 'o_c_id', 'o_ol_cnt','o_carrier_id', 'o_all_local', 'o_entry_d']
    order_df.to_csv('order_df.csv',index=False)

def process_orders_by_customer():
    orders_df = pandas.read_csv('order_df.csv')
    customers_df = pandas.read_csv('customer_df.csv')

    orders_by_customer_df = pandas.merge(customers_df, orders_df,
                                         left_on=['c_w_id', 'c_d_id', 'c_id'],
                                         right_on=['o_w_id', 'o_d_id', 'o_c_id'],
                                         how='inner')
    orders_by_customer_df = orders_by_customer_df.loc[:, ['c_w_id', 'c_d_id', 'c_id', 'o_id', 'o_carrier_id', 'o_entry_d']]
    orders_by_customer_df.to_csv('orders_by_customer_df.csv', index=False)

def process_order_line():
    order_line_df = pandas.read_csv('order-line.csv', header=None)
    order_line_df.columns = ['ol_w_id', 'ol_d_id', 'ol_o_id', 'ol_number', 'ol_i_id', 'ol_delivery_d', 'ol_amount', 'ol_supply_w_id', 'ol_quantity', 'ol_dist_info']
    order_line_df.to_csv('order_line_df.csv',index=False)

def process_stock():
    stock_df = pandas.read_csv('stock.csv', header=None)
    stock_df.columns = ['s_w_id', 's_i_id', 's_quantity', 's_ytd', 's_order_cnt','s_remote_cnt',
                        's_dist_01', 's_dist_02', 's_dist_03', 's_dist_04', 's_dist_05', 's_dist_06', 's_dist_07', 's_dist_08',
                        's_dist_09', 's_dist_10', 's_data'
                        ]
    stock_df.to_csv('stock_df.csv',index=False)

def process_customer():
    customer_df = pandas.read_csv('customer.csv', header=None)
    customer_df.columns = ['c_w_id', 'c_d_id', 'c_id', 'c_first', 'c_middle', 'c_last',
                           'street_1', 'street_2', 'city',
                           'state', 'zip',
                           'c_phone', 'c_since', 'c_credit', 'c_credit_lim',
                           'c_discount', 'c_balance',
                           'c_ytd_payment', 'c_payment_cnt',
                           'c_delivery_cnt', 'c_data']
    customer_df['c_address'] = customer_df.apply(merge_cells, axis=1)
    customer_df['c_name'] = customer_df.apply(merge_name, axis=1)
    customer_df = customer_df.drop(['c_first', 'c_middle', 'c_last', 'street_1', 'street_2', 'city', 'state', 'zip'], axis=1)
    customer_df.to_csv('customer_df.csv',index=False)


def process_top_balances():
    customer_df = pandas.read_csv('customer_df.csv').loc[:, ['c_id', 'c_w_id', 'c_d_id', 'c_balance', 'c_name']]
    warehouses_df = pandas.read_csv('warehouse_df.csv').loc[:, ['w_id', 'w_name']]
    districts_df = pandas.read_csv('district_df.csv').loc[:, ['d_w_id','d_id', 'd_name']]
    merged_df = pandas.merge(customer_df, districts_df, left_on=['c_d_id', 'c_w_id'], right_on=['d_id', 'd_w_id'], how='inner')
    final_merged_df = pandas.merge(merged_df, warehouses_df, left_on=['c_w_id'], right_on=['w_id'], how='inner')
    final_merged_df['dummy_partition_key'] = 'global'
    final_merged_df = final_merged_df.drop('d_id', axis=1)
    final_merged_df = final_merged_df.drop('d_w_id', axis=1)
    final_merged_df = final_merged_df.drop('w_id', axis=1)
    final_merged_df.to_csv('top_balances_df.csv', index=False)

def check_rows():
    customer_rows = pandas.read_csv('customer.csv').shape[0]
    print(f"The customers CSV file has {customer_rows} rows.")

    customer_df_rows = pandas.read_csv('customer_df.csv').shape[0]
    print(f"The customers df CSV file has {customer_df_rows} rows.")

    num_rows = pandas.read_csv('top_balances_df.csv').shape[0]
    print(f"The top_balances_df CSV file has {num_rows} rows.")



if __name__ == '__main__':
    # process_warehouse()
    # process_district()
    # process_items()
    # process_order()
    # process_order_line()
    # process_stock()
    # process_customer()
    # process_orders_by_customer()
    # process_top_balances()
    check_rows()

    

