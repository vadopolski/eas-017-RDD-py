# transformations (descriptions of computations) vs actions (force the evaluation of a DF/RDD)
# 1 action => 1 job
# 1 job => many stages
# 1 stage => many tasks


# Spark
# List of problems
# - map reduce implementation - using memory or local FS instead HDFS
# - fault tolerance in distributed mode
# - how to scale spark cluster (slot, reducers, cores per reducers, max cores, dynamic allocation)
# - how to debug, how to increase performance (local, cluster, client mode)
# - how to get resource in cluster (kubernetus, yarn, mesos, standalone)
# - how to get and put data from/into anywhere in paralllel mode
# - how can Spark resolve 3 classic big data issues: word count, I/O in parallel, sorting data
# - how to control distributed processing with Spark (Spark UI)
# - how to choose streaming or batch
# - how to restart Spark automatically


# executors_num
# memory_per_ex
# cores_per_execute
# s = executors_num * cores_per_execut = 400 slotes
#  20 block => 20 slotes ~ 95%
