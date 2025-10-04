import sys
from pyspark.sql import SparkSession

def main(input_path, output_path):
    spark = SparkSession.builder.appName("compact_covid").getOrCreate()
    
    df = spark.read.json(input_path)
    
    df.write.mode("overwrite") \
      .option("compression","snappy") \
      .parquet(output_path)
    spark.stop()

if __name__ == "__main__":
    _, inp, outp = sys.argv
    main(inp, outp)
