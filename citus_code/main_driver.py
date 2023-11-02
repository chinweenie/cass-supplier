import csv 
import sys
import numpy as np 

# define csv name and set check for file header
csv_name = "./result/clients.csv"
file_exist = os.path.isfile(csv_name)

# define header for clients.csv 
header = ['client_number', 'transaction_num', 'total_time', 'transaction_throughput', 'avg_latency', 'median_latency', '95th_pct_latency', '99th_pct_latency']

sys.path.append("./transactions/")
# get client_no from bash and remove .txt
# file_path = sys.argv[1]
# file_name = file_path.split('/')[-1]
# file_prefix = file_name.split('.')[0]

from deliver import deliver
from top_balance import top_balance
from order_status import order_status
from related_customer import related_customer

# parameters link to citus 
host="localhost"
database="project"
user="cs4224d"
password="1234"

loop_count = 0 
total_time = 0 
transaction_count = 0
transaction_latency_list = []

# read one file via stdin 
# a bash is required for txt file input 
for line in sys.stdin:
    loop_count += 1
    print("read line:", loop_count) 
    if not line: 
        break
    else: 
        transaction_message = line.strip().split(',')
        transaction_type = transaction_message[0]
        transaction_count += 1 
        # print('loop:',loop_count)
        # print(transaction_type)

    # python do not support switch, so simply implement if&elif 
    # execute deliver transaction 
    if transaction_type == 'D': 
        warehouse_id, carrier_id= transaction_message[1], transaction_message[2]
        # latency count in ms 
        latency = deliver(host, database, user, password, warehouse_id, carrier_id)
    
    # execute top_up transaction
    elif transaction_type == 'T':
        latency = top_balance(host, database, user, password)

    # execute order_status transaction
    elif transaction_type == 'O':
        c_w_id, c_d_id, c_id = transaction_message[1], transaction_message[2], transaction_message[3]
        latency = order_status(host, database, user, password, c_w_id, c_d_id, c_id)

    # execute related_customer transaction
    elif transaction_type == 'R':
        c_w_id, c_d_id, c_id = transaction_message[1], transaction_message[2], transaction_message[3]
        latency = related_customer(host, database, user, password, c_w_id, c_d_id, c_id)

    # please add your functions below 


    transaction_latency_list.append(latency)
    total_time += latency

# performance measurement
average_latency = total_time/transaction_count
total_time = total_time/1000
transaction_throughput = transaction_count / total_time

transaction_latency_list.sort()
np_latency_data = np.array(transaction_latency_list)
median_latency = np.median(np_latency_data)
percentile_95th_latency = np.percentile(np_latency_data, 95)
percentile_99th_latency = np.percentile(np_latency_data, 99)

# output measurement for each client and write into csv 
# print(f"client {file_name} finish:")
print(f"(a) Number of executed transactions: {transaction_count}")
print(f"(b) Total transaction execution time(s): {total_time}")
print(f"(c) Transaction throughput(num/s): {transaction_throughput}")
print(f"(d) Average transaction latency(in ms): {average_latency}")
print(f"(e) Median transaction latency(in ms): {median_latency}")
print(f"(f) 95th percentile transaction latency(in ms): {percentile_95th_latency}")
print(f"(f) 99th percentile transaction latency(in ms): {percentile_99th_latency}")

# build up data for csv row 
data = [] 

with open(csv_name, mode='a', newline='') as file: 
    writer = csv.writer(file)

    # first time to create csv, create header 
    if not file_exist:
        writer.writerow(header)
    
    # write in data 
    writer.writerow(data)
    
