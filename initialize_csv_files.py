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
    warehouse_df = pandas.read_csv('warehouse.csv', header=None, dtype=str)
    warehouse_df.columns = ['w_id', 'w_name', 'street_1', 'street_2', 'city', 'state', 'zip', 'w_tax',
                            'w_ytd']
    warehouse_df['w_address'] = warehouse_df.apply(merge_cells, axis=1)
    warehouse_df = warehouse_df.drop(['street_1', 'street_2', 'city', 'state', 'zip'], axis=1)
    warehouse_df.columns = [col.upper() for col in warehouse_df.columns]
    warehouse_df.to_csv('warehouse_df.csv',index=False, sep=',')


def process_district():
    district_df = pandas.read_csv('district.csv', header=None)
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

def process_order_line():
    df = pandas.read_csv('order-line.csv', header=None)
    order_line_df = pandas.read_csv('order-line.csv')
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

def process_undelivered_orders_by_warehouse_district():
    orders_by_wd_df = pandas.read_csv('order_df.csv')
    is_undelivered = orders_by_wd_df['o_carrier_id'].isnull()
    undelivered_orders_by_wd_df = orders_by_wd_df[is_undelivered]
    undelivered_orders_by_wd_df = undelivered_orders_by_wd_df.drop(['o_ol_cnt', 'o_all_local', 'o_entry_d'], axis=1)

    undelivered_orders_by_wd_df.to_csv('undelivered_orders.csv', index=False)

def process_related_customers_txns():
    orders_df = pandas.read_csv('order_df.csv')
    orders_df = orders_df.drop(['o_carrier_id', 'o_ol_cnt', 'o_all_local', 'o_entry_d'], axis=1)

    order_line_df = pandas.read_csv('order_line_df.csv')
    order_line_df = order_line_df.drop(['ol_delivery_d', 'ol_amount', 'ol_supply_w_id', 'ol_quantity', 'ol_dist_info'], axis=1)

    # rename columns in order and order_line for inner join
    orders_df.rename(columns={'o_w_id': 'w_id', 'o_d_id': 'd_id'}, inplace=True)
    order_line_df.rename(columns={'ol_w_id': 'w_id', 'ol_d_id': 'd_id', 'ol_o_id': 'o_id'}, inplace=True)

    order_join_orderline_df = pandas.merge(orders_df, order_line_df, on=['w_id', 'd_id', 'o_id'])

    print('order_join_orderline_df size: ' + str(order_join_orderline_df.shape))
    order_join_orderline_df.to_csv('order_join_orderline_df.csv', index=False)

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

        cross_df = cross_df.drop(['w_id1', 'd_id1', 'o_id1', 'o_c_id1', 'ol_number1'], axis=1)
        cross_df.rename(columns={'o_c_id': 'c_id', 'ol_i_id': 'i1_id', 'ol_i_id1': 'i2_id'}, inplace=True)

        cross_df = cross_df.reset_index(drop=True)
        two_item_df = two_item_df.reset_index(drop=True)
        two_item_df = pandas.concat([two_item_df, cross_df], axis=0, ignore_index=True)
        
        group_count_tracker += 1
        print("% processed: " + str(group_count_tracker / grouped.ngroups))
    
    print('two_item_df.shape: ' + str(two_item_df.shape))
    two_item_df.to_csv('two_item_df.csv', index=False)
    '''
    
if __name__ == '__main__':
    
    process_warehouse()
    process_district()
    process_items()
    process_order()
    process_order_line()
    process_stock()
    process_customer()
    process_undelivered_orders_by_warehouse_district()
    process_related_customers_txns()
    

