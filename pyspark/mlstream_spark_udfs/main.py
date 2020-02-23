from pyspark.sql.session import SparkSession
import mlstream_spark_udfs as udfs

def main():
  print(udfs.jarPath())
  
if __name__== "__main__":
  main()

