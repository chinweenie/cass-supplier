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
    order_df.columns = ['o_w_id', 'o_d_id', 'o_id', 'o_c_id', 'o_carrier_id', 'o_ol_cnt', 'o_all_local', 'o_entry_d']
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
# for txn 2.3
def process_undelivered_orders_by_warehouse_district():
    orders_by_wd_df = pandas.read_csv('order_df.csv')
    is_undelivered = orders_by_wd_df['o_carrier_id'].isnull()
    undelivered_orders_by_wd_df = orders_by_wd_df[is_undelivered]
    undelivered_orders_by_wd_df = undelivered_orders_by_wd_df.drop(['o_ol_cnt', 'o_all_local', 'o_entry_d'], axis=1)

    undelivered_orders_by_wd_df.to_csv('undelivered_orders.csv', index=False)

# for txn 2.8
def process_related_customers_txns():
    orders_df = pandas.read_csv('order_df.csv')
    orders_df = orders_df.drop(['o_carrier_id', 'o_ol_cnt', 'o_all_local', 'o_entry_d'], axis=1)

    order_line_df = pandas.read_csv('order_line_df.csv')
    order_line_df = order_line_df.drop(['ol_delivery_d', 'ol_amount', 'ol_supply_w_id', 'ol_quantity', 'ol_dist_info'], axis=1)

    # rename columns in order and order_line for inner join
    orders_df.rename(columns={'o_w_id': 'w_id', 'o_d_id': 'd_id', 'o_c_id': 'c_id'}, inplace=True)
    order_line_df.rename(columns={'ol_w_id': 'w_id', 'ol_d_id': 'd_id', 'ol_o_id': 'o_id', 'ol_i_id': 'i_id'}, inplace=True)

    orders_by_warehouse_district_customer_df = pandas.merge(orders_df, order_line_df, on=['w_id', 'd_id', 'o_id'])
    
    new_ordering = ['w_id', 'd_id', 'c_id', 'o_id', 'ol_number', 'i_id']
    orders_by_warehouse_district_customer_df = orders_by_warehouse_district_customer_df[new_ordering]

    # this table is clustered by these 3 columns, w_id, d_id, c_id
    orders_by_warehouse_district_customer_df = orders_by_warehouse_district_customer_df.sort_values(by=['w_id', 'd_id', 'c_id'])

    print('orders_by_warehouse_district_customer_df size: ' + str(orders_by_warehouse_district_customer_df.shape))
    orders_by_warehouse_district_customer_df.to_csv('orders_by_warehouse_district_customer_df.csv', index=False)

    '''
    two_item_column_names = ['w_id', 'd_id', 'o_id', 'c_id', 'ol_number', 'i1_id', 'i2_id']
    two_item_df = pandas.DataFrame(columns=two_item_column_names)
    grouped = order_join_orderline_df.groupby(['w_id', 'd_id', 'o_id', 'o_c_id'])

    group_count_tracker = 0

    for name, group in grouped:
        group_copy = group.copy()
        group_copy.rename(columns={'w_id': 'w_id1', 'd_id': 'd_id1', 'o_id': 'o_id1','o_c_id': 'o_c_id1', 'ol_number': 'ol_number1', 'ol_i_id': 'ol_i_id1'}, inplace=True)
        cross_df = pandas.merge(group, group_copy, how='cross')

        item_filter = (cross_df['ol_i_id'] != cross_df['ol_i_id1'])
        cross_df = cross_df[item_filter]

    # print(row_counts)
    customer_df.to_csv('customer_df.csv',index=False)

        cross_df = cross_df.reset_index(drop=True)
        two_item_df = two_item_df.reset_index(drop=True)
        two_item_df = pandas.concat([two_item_df, cross_df], axis=0, ignore_index=True)
        
        group_count_tracker += 1
        print("% processed: " + str(group_count_tracker / grouped.ngroups))
    
    print('two_item_df.shape: ' + str(two_item_df.shape))
    two_item_df.to_csv('two_item_df.csv', index=False)
    '''

# for txn 2.5
def process_storage_under_threshold():
    order_line_df = pandas.read_csv("df_files/order_line_df.csv")
    stock_df = pandas.read_csv("df_files/stock_df.csv")

    stock_df = stock_df[['s_w_id', 's_i_id', 's_quantity']].rename(
        columns={'s_w_id': 'w_id', 's_i_id': 'i_id'})
    order_line_df = order_line_df[['ol_w_id', 'ol_d_id', 'ol_o_id', 'ol_number', 'ol_i_id']].rename(
        columns={'ol_w_id': 'w_id', 'ol_d_id': 'd_id', 'ol_i_id': 'i_id'})

    df = pandas.merge(order_line_df, stock_df, how='inner')
    df.to_csv('storage_under_treshold.csv', index=False)
    
if __name__ == '__main__':
    #check_rows()   
    process_warehouse()
    process_district()
    process_items()
    process_order()
    process_order_line()
    process_stock()
    process_customer()
    process_orders_by_customer()
    process_top_balances()
    process_undelivered_orders_by_warehouse_district()
    process_related_customers_txns()
    process_storage_under_threshold()
    

