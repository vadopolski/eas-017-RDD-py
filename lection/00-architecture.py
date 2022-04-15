# transformations (descriptions of computations) vs actions (force the evaluation of a DF/RDD)
# 1 action => 1 job
# 1 job => many stages
# 1 stage => many tasks


# Spark UI


    groups = cachedInts.map { lambda x:
            if x % 2 == 0
                (0, x)
            else
                (1, x)
    }

print("== Group");
print(groups.groupByKey().toDebugString)

val
array = groups.groupByKey().coalesce(3).collect()
array.foreach(print)
val
intToLong: collection.Map[Int, Long] = groups.countByKey()
print(intToLong.toString())

print("== Actions")
print("First elem is " + cachedInts.first())
print("Count is " + cachedInts.count())
print("Take(2)")

cachedInts.take(2).foreach(print)
print("Take ordered (5)")
cachedInts.takeOrdered(5).foreach(print)
