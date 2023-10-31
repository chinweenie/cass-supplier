import statistics
import decimal
import csv

from cassandra.cluster import Cluster
from cassandra.query import tuple_factory
from cassandra import ConsistencyLevel
from cassandra.policies import TokenAwarePolicy, RoundRobinPolicy, DowngradingConsistencyRetryPolicy
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, ExecutionProfile

if __name__ == '__main__':
    cluster_profile = ExecutionProfile(
        load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()),
        consistency_level=ConsistencyLevel.QUORUM,
        retry_policy=DowngradingConsistencyRetryPolicy(),
        request_timeout=120
    )
    cluster = Cluster(['0.0.0.0'], port=9042)
    session = cluster.connect("supplier")
    session.row_factory = tuple_factory
    directory = '/home/stuproj/cs4224d'

    queries = ["select sum(W_YTD) from Warehouses", 
               "select sum(D_YTD), sum(D_NEXT_O_ID) from Districts",
               "select sum(C_BALANCE), sum(C_YTD_PAYMENT), sum(C_PAYMENT_CNT), sum(C_DELIVERY_CNT) from Customers",
               "select max(O_ID), sum(O_OL_CNT) from Orders",
               "select sum(OL_AMOUNT), sum(OL_QUANTITY) from Order_Lines",
               "select sum(S_QUANTITY), sum(S_YTD), sum(S_ORDER_CNT), sum(S_REMOTE_CNT) from Stocks"]
    
    res = ["sum(W_YTD)", "sum(D_YTD)", "sum(D_NEXT_O_ID)", "sum(C_BALANCE)", "sum(C_YTD_PAYMENT)", 
           "sum(C_PAYMENT_CNT)", "sum(C_DELIVERY_CNT)", "max(O_ID)", "sum(O_OL_CNT)", "sum(OL_AMOUNT)", 
           "sum(OL_QUANTITY)", "sum(S_QUANTITY)", "sum(S_YTD)", "sum(S_ORDER_CNT)", "sum(S_REMOTE_CNT)"]
    
    futures = [None]*len(queries)
    results = []
    for i, q in enumerate(queries):
        futures[i] = session.execute_async(q)
    
    throughputs = []
    with open(f"{directory}client.csv", 'r') as file:
        reader = csv.reader(file, delimiter=',')
        next(reader)
        for row in reader:
            throughputs.append(float(row[3]))

    with open(f"{directory}throughput.csv", 'w') as f:
        writer = csv.writer(f)
        if f.tell() == 0:
            writer.writerow(["min_throughput", "max_throughput","avg_throughput"])
        writer.writerow([min(throughputs), max(throughputs), statistics.mean(throughputs)])

    for future in futures:
        result = map(lambda x: float(x) if (type(x) == decimal.Decimal) else x, list(future.result().one()))
        results.extend(list(result))    
    with open(f"{directory}dbstate.csv", 'w') as f:
        writer = csv.writer(f)
        for i, v in enumerate(res):
            writer.writerow([v, results[i]])
