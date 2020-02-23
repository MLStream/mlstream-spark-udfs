from pyspark.sql import *
import mlstream_spark_udfs as udfs
import timeit

spark = SparkSession.builder.getOrCreate()
udfs.registerUDFs(spark)

def t(f):
    return timeit.timeit(f, number=1)

results = []

def query(qry):
   tm = t(lambda: spark.sql(qry).show())
   print("Query {} took {} secs.".format(qry, tm))
   results.append((qry, tm))

# 1 billion, but just 7.6 GBs if stored as longs
inputs = 10000 # 1000000000
spark.range(0, inputs,  step = 1, numPartitions = 4).createOrReplaceTempView("raw_input")
spark.sql("SELECT (case when id % 2 = 0 then 0 else hash(id) end) as id FROM raw_input").createOrReplaceTempView("test_table")

def topk_query(key_expr, memory):
	return \
"""SELECT extract_approx_topk(distribution) 
FROM (
	SELECT 
	approx_topk({key_expr}, CAST(1.0 AS FLOAT), 10, "{memory}")
	AS distribution from test_table
) tmp""".format(key_expr=key_expr, memory = memory)

queries = [
"""SELECT COUNT(*) from test_table where id = 0""",
topk_query("CAST (id AS INTEGER)", "100MB"),
topk_query("CAST (id AS LONG)", "100MB"),
topk_query("CAST (id AS STRING)", "100MB"),
topk_query("struct(CAST (id AS STRING), CAST (id AS INTEGER))", "10MB") # runs out of memory if more than 10MBs
]

for q in queries:
    query(q)

print("\n===============================")
print("Results")   	
for (q, tm) in results:
    print("{}: {}".format(tm, q))


"""
Summary of results
===============================
Results
2.8767172889999983: SELECT COUNT(*) from test_table where id = 0

100.85104866699999: SELECT extract_approx_topk(distribution)
FROM (
	SELECT
	approx_topk(CAST (id AS INTEGER), CAST(1.0 AS FLOAT), 10, "100MB")
	AS distribution from test_table
) tmp

149.04900114800012: SELECT extract_approx_topk(distribution)
FROM (
	SELECT
	approx_topk(CAST (id AS LONG), CAST(1.0 AS FLOAT), 10, "100MB")
	AS distribution from test_table
) tmp

174.84902018399998: SELECT extract_approx_topk(distribution)
FROM (
	SELECT
	approx_topk(CAST (id AS STRING), CAST(1.0 AS FLOAT), 10, "100MB")
	AS distribution from test_table
) tmp

172.64252342499958: SELECT extract_approx_topk(distribution)
FROM (
	SELECT
	approx_topk(struct(CAST (id AS STRING), CAST (id AS INTEGER)), CAST(1.0 AS FLOAT), 10, "10MB")
	AS distribution from test_table
) tmp
"""
