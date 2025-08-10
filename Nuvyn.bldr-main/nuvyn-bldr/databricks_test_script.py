# Test 1: Basic Setup
print("�� Starting Amazon ELT Pipeline Test")

# Test 2: Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, trim, when
from pyspark.sql.types import StringType, DoubleType, IntegerType

print("✅ Libraries imported successfully")

# Test 3: Initialize Spark
spark = SparkSession.builder \
    .appName("Amazon Product Reviews ELT Pipeline") \
    .getOrCreate()

print("✅ Spark session created")

# Test 4: Read Amazon Data
# Update this path to your uploaded file location
source_data_path = "/FileStore/tables/amazon.csv"  # Update this path
source_df = spark.read.csv(source_data_path, header=True, inferSchema=True)

print(f"✅ Data loaded: {source_df.count()} rows, {len(source_df.columns)} columns")
print(f"📊 Columns: {source_df.columns}")

# Test 5: Show Sample Data
print("\n📋 Sample Data:")
source_df.show(5)

# Test 6: Basic Transformations
dim_user_df = source_df.select(
    col("user_id"),
    trim(upper(col("user_name"))).alias("user_name")
).distinct()

print(f"✅ Dimension table created: {dim_user_df.count()} unique users")

# Test 7: Data Quality Check
null_check = source_df.filter(col("product_id").isNull()).count()
print(f"✅ Data quality check: {null_check} null product_ids found")

print("🎉 All tests completed successfully!")