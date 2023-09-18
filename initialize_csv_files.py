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
    warehouse_df = pandas.read_csv('warehouse.csv', dtype=str)
    warehouse_df.columns = ['w_id', 'w_name', 'street_1', 'street_2', 'city', 'state', 'zip', 'w_tax',
                            'w_ytd']
    warehouse_df['w_address'] = warehouse_df.apply(merge_cells, axis=1)
    warehouse_df = warehouse_df.drop(['street_1', 'street_2', 'city', 'state', 'zip'], axis=1)
    warehouse_df.columns = [col.upper() for col in warehouse_df.columns]
    warehouse_df.to_csv('warehouse_df.csv',index=False, sep=',')


def process_district():
    district_df = pandas.read_csv('district.csv')
    district_df.columns = ['d_w_id', 'd_id', 'd_name', 'street_1', 'street_2', 'city', 'state', 'zip',
                           'd_tax', 'd_ytd', 'd_next_o_id']
    district_df['address'] = district_df.apply(merge_cells, axis=1)
    district_df = district_df.drop(['street_1', 'street_2', 'city', 'state', 'zip'], axis=1)
    district_df.to_csv('district_df.csv',index=False)

def process_items():
    item_df = pandas.read_csv('item.csv')
    item_df.columns = ['i_id', 'i_name', 'i_price', 'i_im_id', 'i_data']
    item_df.to_csv('item_df.csv',index=False)

def process_order():
    order_df = pandas.read_csv('order.csv')
    order_df.columns = ['o_w_id', 'o_d_id', 'o_id', 'o_c_id', 'o_carrier_id', 'o_ol_cnt', 'o_all_local', 'o_entry_d']
    order_df.to_csv('order_df.csv',index=False)

def process_order_line():
    df = pandas.read_csv('order_line_df.csv')
    print(df.shape)
    # order_line_df = pandas.read_csv('order-line.csv')
    # order_line_df.columns = ['ol_w_id', 'ol_d_id', 'ol_o_id', 'ol_number', 'ol_i_id', 'ol_delivery_d', 'ol_amount', 'ol_supply_w_id', 'ol_quantity', 'ol_dist_info']
    # order_line_df.to_csv('order_line_df.csv',index=False)

def process_stock():
    stock_df = pandas.read_csv('stock.csv')
    stock_df.columns = ['s_w_id', 's_i_id', 's_quantity', 's_ytd', 's_order_cnt','s_remote_cnt',
                        's_dist_01', 's_dist_02', 's_dist_03', 's_dist_04', 's_dist_05', 's_dist_06', 's_dist_07', 's_dist_08',
                        's_dist_09', 's_dist_10', 's_data'
                        ]
    stock_df.to_csv('stock_df.csv',index=False)

def process_customer():
    customer_df = pandas.read_csv('customer.csv')
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
    # row_counts = customer_df.count(axis=1)

    # print(row_counts)
    customer_df.to_csv('customer_df.csv',index=False)


if __name__ == '__main__':
    process_warehouse()
    process_district()
    process_items()
    process_order()
    process_order_line()
    process_stock()
    process_customer()

