import sys
sys.path.append("./transactions/")

from deliver import deliver
from top_balance import top_balance
from order_status import order_status
from related_customer import related_customer

# loop_count = 0 
transaction_count = 0
total_time = 0 
transaction_throughput = 0 
transaction_latency_list = []

# read one file via stdin 
# a bash is required for txt file input 
for line in sys.stdin:
    # loop_count += 1 
    if not line: 
        break
    else: 
        transaction_message = line.strip().split(',')
        transaction_type = transaction_message[0]
        # print('loop:',loop_count)
        # print(transaction_type)

    # python do not support switch, so simply implement if&elif 
    if transaction_type == 'D': 
        
