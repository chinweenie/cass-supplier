import json

import numpy
import pandas

FILEDIR = "/temp/teamd-cass/apache-cassandra-4.1.3/bin/data_files/"
# FILEDIR = "~/Desktop/CassandraProj/data_files/"


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
    filename = FILEDIR + 'warehouse.csv'
    warehouse_df = pandas.read_csv(filename, dtype=str, header=None)
    warehouse_df.columns = ['w_id', 'w_name', 'street_1', 'street_2', 'city', 'state', 'zip', 'w_tax',
                            'w_ytd']
    warehouse_df['w_address'] = warehouse_df.apply(merge_cells, axis=1)
    warehouse_df = warehouse_df.drop(['street_1', 'street_2', 'city', 'state', 'zip'], axis=1)
    warehouse_df.to_csv(FILEDIR + 'warehouse_df.csv', index=False, sep=',')


def process_district():
    district_df = pandas.read_csv(FILEDIR + 'district.csv', header=None)
    district_df.columns = ['d_w_id', 'd_id', 'd_name', 'street_1', 'street_2', 'city', 'state', 'zip',
                           'd_tax', 'd_ytd', 'd_next_o_id']
    district_df['address'] = district_df.apply(merge_cells, axis=1)
    district_df = district_df.drop(['street_1', 'street_2', 'city', 'state', 'zip'], axis=1)
    district_df.to_csv(FILEDIR + 'district_df.csv', index=False)


def process_items():
    item_df = pandas.read_csv(FILEDIR + 'item.csv', header=None)
    item_df.columns = ['i_id', 'i_name', 'i_price', 'i_im_id', 'i_data']
    item_df.to_csv(FILEDIR + 'item_df.csv', index=False)


def int_or_nan(val):
    try:
        return int(val)
    except:
        return numpy.nan


def process_order():
    order_df = pandas.read_csv(FILEDIR + 'order.csv', header=None)
    order_df.columns = ['o_w_id', 'o_d_id', 'o_id', 'o_c_id', 'o_carrier_id', 'o_ol_cnt', 'o_all_local', 'o_entry_d']

    order_df['o_carrier_id'] = order_df['o_carrier_id'].astype('Int64')
    order_df.to_csv(FILEDIR + 'order_df.csv', index=False)


def process_orders_by_customer():
    orders_df = pandas.read_csv(FILEDIR + 'order_df.csv', low_memory=False)

    customers_df = pandas.read_csv(FILEDIR + 'customer_df.csv', low_memory=False)
    orders_by_customer_df = pandas.merge(customers_df, orders_df,
                                         left_on=['c_w_id', 'c_d_id', 'c_id'],
                                         right_on=['o_w_id', 'o_d_id', 'o_c_id'],
                                         how='inner')
    orders_by_customer_df = orders_by_customer_df.loc[:,
                            ['c_w_id', 'c_d_id', 'c_id', 'o_id', 'o_carrier_id', 'o_entry_d']]
    orders_by_customer_df['o_carrier_id'] = orders_by_customer_df['o_carrier_id'].astype('Int64')

    orders_by_customer_df.to_csv(FILEDIR + 'orders_by_customer_df.csv', index=False)


def process_order_line():
    order_line_df = pandas.read_csv(FILEDIR + 'order-line.csv', header=None)
    order_line_df.columns = ['ol_w_id', 'ol_d_id', 'ol_o_id', 'ol_number', 'ol_i_id', 'ol_delivery_d', 'ol_amount',
                             'ol_supply_w_id', 'ol_quantity', 'ol_dist_info']
    order_line_df.to_csv(FILEDIR + 'order_line_df.csv', index=False)


def process_stock():
    stock_df = pandas.read_csv(FILEDIR + 'stock.csv', header=None)
    stock_df.columns = ['s_w_id', 's_i_id', 's_quantity', 's_ytd', 's_order_cnt', 's_remote_cnt',
                        's_dist_01', 's_dist_02', 's_dist_03', 's_dist_04', 's_dist_05', 's_dist_06', 's_dist_07',
                        's_dist_08',
                        's_dist_09', 's_dist_10', 's_data'
                        ]
    stock_df.to_csv(FILEDIR + 'stock_df.csv', index=False)


def process_customer():
    customer_df = pandas.read_csv(FILEDIR + 'customer.csv', header=None)
    customer_df.columns = ['c_w_id', 'c_d_id', 'c_id', 'c_first', 'c_middle', 'c_last',
                           'street_1', 'street_2', 'city',
                           'state', 'zip',
                           'c_phone', 'c_since', 'c_credit', 'c_credit_lim',
                           'c_discount', 'c_balance',
                           'c_ytd_payment', 'c_payment_cnt',
                           'c_delivery_cnt', 'c_data']
    customer_df['c_address'] = customer_df.apply(merge_cells, axis=1)
    customer_df['c_name'] = customer_df.apply(merge_name, axis=1)
    customer_df = customer_df.drop(['c_first', 'c_middle', 'c_last', 'street_1', 'street_2', 'city', 'state', 'zip'],
                                   axis=1)
    customer_df.to_csv(FILEDIR + 'customer_df.csv', index=False)


def process_top_balances():
    customer_df = pandas.read_csv(FILEDIR + 'customer_df.csv').loc[:,
                  ['c_id', 'c_w_id', 'c_d_id', 'c_balance', 'c_name']]
    warehouses_df = pandas.read_csv(FILEDIR + 'warehouse_df.csv').loc[:, ['w_id', 'w_name']]
    districts_df = pandas.read_csv(FILEDIR + 'district_df.csv').loc[:, ['d_w_id', 'd_id', 'd_name']]
    merged_df = pandas.merge(customer_df, districts_df, left_on=['c_d_id', 'c_w_id'], right_on=['d_id', 'd_w_id'],
                             how='inner')
    final_merged_df = pandas.merge(merged_df, warehouses_df, left_on=['c_w_id'], right_on=['w_id'], how='inner')
    final_merged_df['dummy_partition_key'] = 'global'
    final_merged_df = final_merged_df.drop('d_id', axis=1)
    final_merged_df = final_merged_df.drop('d_w_id', axis=1)
    final_merged_df = final_merged_df.drop('w_id', axis=1)
    final_merged_df.to_csv(FILEDIR + 'top_balances_df.csv', index=False)


def check_rows():
    warehouse_rows = pandas.read_csv(FILEDIR+'warehouse.csv',header=None).shape[0]
    warehouse_df = pandas.read_csv(FILEDIR+'warehouse_df.csv',header=None).shape[0]-1
    print(f"warehouse_rows:{warehouse_rows},warehouse_df:{warehouse_df} .")

    customer_rows = pandas.read_csv(FILEDIR+'customer.csv', header=None).shape[0]
    customer_df_rows = pandas.read_csv(FILEDIR+'customer_df.csv', header=None).shape[0]-1
    num_rows = pandas.read_csv('top_balances_df.csv', header=None).shape[0]-1
    print(f"customer_rows:{customer_rows}, customer_df_rows:{customer_df_rows}, top_balances_df:{num_rows}")

    order_rows = pandas.read_csv(FILEDIR+'order.csv', header=None).shape[0]
    order_df_rows = pandas.read_csv(FILEDIR+'order_df.csv',header=None).shape[0]-1
    print(f"order_rows: {order_rows} rows, order_df_rows:{order_df_rows} rows.")



# for txn 2.3
def process_undelivered_orders_by_warehouse_district():
    orders_by_wd_df = pandas.read_csv(FILEDIR + 'order_df.csv')
    is_undelivered = orders_by_wd_df['o_carrier_id'].isnull()
    undelivered_orders_by_wd_df = orders_by_wd_df[is_undelivered]
    undelivered_orders_by_wd_df = undelivered_orders_by_wd_df.drop(['o_ol_cnt', 'o_all_local', 'o_entry_d'], axis=1)

    undelivered_orders_by_wd_df.to_csv(FILEDIR + 'undelivered_orders.csv', index=False)


# for txn 2.8
def process_related_customers_txns():
    orders_df = pandas.read_csv(FILEDIR + 'order_df.csv')
    orders_df = orders_df.drop(['o_carrier_id', 'o_ol_cnt', 'o_all_local', 'o_entry_d'], axis=1)

    order_line_df = pandas.read_csv(FILEDIR + 'order_line_df.csv')
    order_line_df = order_line_df.drop(['ol_delivery_d', 'ol_amount', 'ol_supply_w_id', 'ol_quantity', 'ol_dist_info'],
                                       axis=1)

    # rename columns in order and order_line for inner join
    orders_df.rename(columns={'o_w_id': 'w_id', 'o_d_id': 'd_id', 'o_c_id': 'c_id'}, inplace=True)
    order_line_df.rename(columns={'ol_w_id': 'w_id', 'ol_d_id': 'd_id', 'ol_o_id': 'o_id', 'ol_i_id': 'i_id'},
                         inplace=True)

    orders_by_warehouse_district_customer_df = pandas.merge(orders_df, order_line_df, on=['w_id', 'd_id', 'o_id'])

    new_ordering = ['w_id', 'd_id', 'c_id', 'o_id', 'ol_number', 'i_id']
    orders_by_warehouse_district_customer_df = orders_by_warehouse_district_customer_df[new_ordering]

    # this table is clustered by these 3 columns, w_id, d_id, c_id
    orders_by_warehouse_district_customer_df = orders_by_warehouse_district_customer_df.sort_values(
        by=['w_id', 'd_id', 'c_id'])

    print('orders_by_warehouse_district_customer_df size: ' + str(orders_by_warehouse_district_customer_df.shape))
    orders_by_warehouse_district_customer_df.to_csv(FILEDIR + 'orders_by_warehouse_district_customer_df.csv',
                                                    index=False)


# for txn 2.5
def process_storage_under_threshold():
    order_line_df = pandas.read_csv(FILEDIR + "order_line_df.csv")
    stock_df = pandas.read_csv(FILEDIR + "stock_df.csv")

    stock_df = stock_df[['s_w_id', 's_i_id', 's_quantity']].rename(
        columns={'s_w_id': 'w_id', 's_i_id': 'i_id'})
    order_line_df = order_line_df[['ol_w_id', 'ol_d_id', 'ol_o_id', 'ol_number', 'ol_i_id']].rename(
        columns={'ol_w_id': 'w_id', 'ol_d_id': 'd_id', 'ol_i_id': 'i_id'})

    df = pandas.merge(order_line_df, stock_df, how='inner')
    df.to_csv(FILEDIR + 'storage_under_treshold.csv', index=False)


# for txn 2.6
def process_popular_items():
    district_df = pandas.read_csv(FILEDIR + "district_df.csv")
    order_df = pandas.read_csv(FILEDIR + "order_df.csv")
    customer_df = pandas.read_csv(FILEDIR + "customer_df.csv")
    item_df = pandas.read_csv(FILEDIR + "item_df.csv")
    order_line_df = pandas.read_csv(FILEDIR + "order_line_df.csv")

    district_df = district_df.loc[:, ['d_w_id', 'd_id', 'd_next_o_id']].rename(
        columns={'d_w_id': 'w_id'})

    order_df = order_df.loc[:, ['o_w_id', 'o_d_id', 'o_id', 'o_c_id', 'o_entry_d']].rename(
        columns={'o_w_id': 'w_id', 'o_d_id': 'd_id', 'o_c_id': 'c_id'})

    customer_df = customer_df.loc[:, ['c_w_id', 'c_d_id', 'c_id', 'c_name']].rename(
        columns={'c_w_id': 'w_id', 'c_d_id': 'd_id'})

    item_df = item_df.loc[:, ['i_id', 'i_name']]

    order_line_df = order_line_df.loc[:,
                    ['ol_w_id', 'ol_d_id', 'ol_o_id', 'ol_number', 'ol_i_id', 'ol_quantity']].rename(
        columns={'ol_w_id': 'w_id', 'ol_d_id': 'd_id', 'ol_o_id': 'o_id', 'ol_i_id': 'i_id'})

    popular_order_line_df = order_line_df
    popular_order_line_df['Rank'] = popular_order_line_df.groupby(
        by=['w_id', 'd_id', 'o_id'])['ol_quantity'].rank('dense')
    popular_order_line_df = popular_order_line_df[popular_order_line_df['Rank'] == 1.0]

    df = pandas.merge(district_df, order_df, how='inner')
    df = pandas.merge(df, customer_df, how='inner')
    df = pandas.merge(df, popular_order_line_df, how='inner')
    df = pandas.merge(df, item_df, how='inner')
    df = df.drop(['c_id', 'Rank', 'd_next_o_id'], axis=1)
    df.to_csv(FILEDIR + 'popular_items_table.csv', index=False)


if __name__ == '__main__':
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
    process_popular_items()
    # check_rows()

    print('csv initialization done')
