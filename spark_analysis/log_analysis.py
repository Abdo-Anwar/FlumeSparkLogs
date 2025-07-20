from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LogAnalysis").getOrCreate()

# CORRECTION: Access SparkContext properly
sc = spark.sparkContext
sc.setLogLevel("WARN")  # Now this will work

rdd = sc.textFile("hdfs://localhost:9000/tmp/logGenED/25-07-19/*")

# Enhanced parser with better error handling
def parse_log(line):
    try:
        parts = line.split(" - ")
        if len(parts) < 4:
            return ("", "", "", 0)
        timestamp = parts[0].strip()
        level = parts[1].strip()
        service = parts[2].strip()
        response_str = parts[3].split()[-1]
        response_time = int(response_str.replace("ms", ""))
        return (timestamp, level, service, response_time)
    except:
        return ("", "", "", 0)  # Return invalid entry for filtering

# Create parsed RDD with explicit filtering
parsed_rdd = rdd.map(parse_log).filter(lambda x: x[3] > 0).cache()

# Run analyses
print("\n" + "="*50)
print(f"Total log entries: {rdd.count()}")

print("\nLog level distribution:")
level_counts = parsed_rdd.map(lambda x: (x[1], 1)).reduceByKey(lambda a,b: a+b).collect()
for level, count in sorted(level_counts):
    print(f"{level}: {count}")

print("\nService distribution:")
service_counts = parsed_rdd.map(lambda x: (x[2], 1)).reduceByKey(lambda a,b: a+b).collect()
for service, count in sorted(service_counts):
    print(f"{service}: {count}")

print("\nAverage response time by service:")
service_times = parsed_rdd.map(lambda x: (x[2], (x[3], 1)))
service_avg = service_times.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])).mapValues(lambda x: x[0]/x[1]).collect()
for service, avg in sorted(service_avg):
    print(f"{service}: {avg:.2f}ms")

print("\nTop 5 slowest responses:")
slowest = parsed_rdd.map(lambda x: (x[3], f"{x[0]} | {x[1]} | {x[2]}")).top(5)
for time, entry in slowest:
    print(f"{time}ms: {entry}")

# Cleanup
parsed_rdd.unpersist()
print("="*50)
spark.stop()