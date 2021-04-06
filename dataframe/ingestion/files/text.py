from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType
import os.path
import yaml

if __name__ == '__main__':
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read Files") \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4') \
        .getOrCreate()
        # .master('local[*]') \
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    print("\nCreating dataframe ingestion CSV file using 'SparkSession.read.format()'")

    crime_schema = StructType() \
        .add("Id", IntegerType(), True) \
        .add("City_Name", StringType(), True) \
        .add("Crime_Name", StringType(), True) \
        .add("Damages", StringType(), True) \
        .add("No_Of_Case", IntegerType(), True) \
        .add("Year", IntegerType(), True) \
        .add("Total_Case", IntegerType(), True)

    crime_df = spark.read \
        .option("header", "false") \
        .option("delimiter", ",") \
        .format("csv") \
        .schema(crime_schema) \
        .load("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/crime_dataset/xac.csv")

    crime_df.printSchema()
    crime_df.show()

    print("Creating dataframe ingestion CSV file using 'SparkSession.read.csv()',")

    crime_report_df = spark.read \
        .option("mode", "DROPMALFORMED") \
        .option("header", "false") \
        .option("delimiter", ",") \
        .option("inferSchema", "true") \
        .csv("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/crime_dataset/xac.csv") \
        .toDF("Id", "City_Name", "Crime_Name", "Damages", "No_Of_Case", "Year", "Total_Case")

    print("Number of partitions = " + str(crime_df.rdd.getNumPartitions()))
    crime_report_df.printSchema()
    crime_report_df.show()

    crime_report_df \
        .repartition(2) \
        .write \
        .partitionBy("id") \
        .mode("overwrite") \
        .option("header", "true") \
        .option("delimiter", "~") \
        .csv("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/crime_rep")

    spark.stop()

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" dataframe/ingestion/files/text.py