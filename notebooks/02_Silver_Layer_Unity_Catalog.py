# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver Layer with Unity Catalog
# MAGIC
# MAGIC This notebook reads Bronze Delta tables from Unity Catalog, cleans and standardizes the data, then writes Silver Delta tables.
# MAGIC
# MAGIC Flow:
# MAGIC
# MAGIC `ecom_db_bharath.bronze` tables -> transformations -> `ecom_db_bharath.silver` tables
# MAGIC

# COMMAND ----------

bronze_schema = "ecom_db_bharath.bronze"
silver_schema = "ecom_db_bharath.silver"

print(bronze_schema)
print(silver_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Spark Functions
# MAGIC
# MAGIC Import PySpark SQL functions and data types used for cleaning, casting, standardization, and derived columns.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze Tables
# MAGIC
# MAGIC Read the Bronze Delta tables by Unity Catalog table name.

# COMMAND ----------

usersDF = spark.read.table(f"{bronze_schema}.users")
buyersDF = spark.read.table(f"{bronze_schema}.buyers")
sellersDF = spark.read.table(f"{bronze_schema}.sellers")
countriesDF = spark.read.table(f"{bronze_schema}.countries")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Bronze Users Data
# MAGIC
# MAGIC Preview the users Bronze table before applying Silver transformations.

# COMMAND ----------

display(usersDF.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Normalize country codes

# COMMAND ----------

usersDF = usersDF.withColumn(
    "countryCode",
    upper(trim(col("countryCode")))
)

# COMMAND ----------

usersDF.select(col("countryCode")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardize Language
# MAGIC
# MAGIC Normalize the existing `language` column and create descriptive language fields.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT language
# MAGIC FROM ecom_db_bharath.bronze.users
# MAGIC ORDER BY language

# COMMAND ----------

# Map language codes into readable labels with a SQL CASE expression.
usersDF = usersDF.withColumn(
    "language_full",
    expr(
        "CASE WHEN language = 'en' THEN 'English' "
        "WHEN language = 'fr' THEN 'French' "
        "WHEN language = 'de' THEN 'German' "
        "WHEN language = 'es' THEN 'Spanish' "
        "WHEN language = 'it' THEN 'Italian' "
        "ELSE 'Other' END"
    )
)

# COMMAND ----------

usersDF.select("language", "language_full").distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Standardize Gender
# MAGIC
# MAGIC Correct potential data entry variations in the `gender` column (e.g. "M", "Male", "m" all become "Male").

# COMMAND ----------

usersDF = usersDF.withColumn(
    "gender",
    when(upper(col("gender")).startswith("M"), "Male")
    .when(upper(col("gender")).startswith("F"), "Female")
    .otherwise("Other")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Clean Civility Title
# MAGIC
# MAGIC Standardize `civilityTitle` values (case-insensitive) into a clean `civilitytitle_clean` column.

# COMMAND ----------

usersDF = usersDF.withColumn(
    "civilitytitle_clean",
    initcap(regexp_replace(lower(col("civilityTitle")), "mme|mrs|ms", "ms"))
)

# COMMAND ----------

usersDF.select("civilityTitle", "civilitytitle_clean").distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Derive Years Since Last Login
# MAGIC
# MAGIC Derive `years_since_last_login` from `daysSinceLastLogin`, handling nulls as 0 days.

# COMMAND ----------

usersDF = usersDF.withColumn(
    "daysSinceLastLogin",
    when(col("daysSinceLastLogin").isNotNull(), col("daysSinceLastLogin").cast(IntegerType()))
    .otherwise(0)
)

usersDF = usersDF.withColumn(
    "years_since_last_login",
    round(col("daysSinceLastLogin") / 365, 2)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Account Age Group
# MAGIC
# MAGIC Calculate age of account in years from `seniority` (days) and bucket into `account_age_group`.

# COMMAND ----------

usersDF = usersDF.withColumn("account_age_years", round(col("seniority") / 365, 2))

usersDF = usersDF.withColumn(
    "account_age_group",
    when(col("account_age_years") < 1, "New")
    .when((col("account_age_years") >= 1) & (col("account_age_years") < 3), "Intermediate")
    .otherwise("Experienced")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Current Year Reference Column
# MAGIC
# MAGIC Add a column with the current year, useful for downstream age/tenure comparisons.

# COMMAND ----------

usersDF = usersDF.withColumn("current_year", year(current_date()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. User Descriptor
# MAGIC
# MAGIC Creatively combine strings to form a unique, human-readable user descriptor.

# COMMAND ----------

usersDF = usersDF.withColumn(
    "user_descriptor",
    concat(
        col("gender"), lit("_"),
        col("countryCode"), lit("_"),
        expr("substring(civilitytitle_clean, 1, 3)"), lit("_"),
        col("language_full")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Flag Long Civility Titles
# MAGIC
# MAGIC Flag records where the raw `civilityTitle` is unusually long (possible data quality issue).

# COMMAND ----------

usersDF = usersDF.withColumn("flag_long_title", length(col("civilityTitle")) > 10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Type Casting
# MAGIC
# MAGIC Cast boolean, integer, and decimal columns to their correct types so downstream Gold aggregations are reliable.

# COMMAND ----------

usersDF = usersDF.withColumn("hasAnyApp", col("hasAnyApp").cast(BooleanType()))
usersDF = usersDF.withColumn("hasAndroidApp", col("hasAndroidApp").cast(BooleanType()))
usersDF = usersDF.withColumn("hasIosApp", col("hasIosApp").cast(BooleanType()))
usersDF = usersDF.withColumn("hasProfilePicture", col("hasProfilePicture").cast(BooleanType()))

usersDF = usersDF.withColumn("socialNbFollowers", col("socialNbFollowers").cast(IntegerType()))
usersDF = usersDF.withColumn("socialNbFollows", col("socialNbFollows").cast(IntegerType()))
usersDF = usersDF.withColumn("socialProductsLiked", col("socialProductsLiked").cast(IntegerType()))
usersDF = usersDF.withColumn("productsListed", col("productsListed").cast(IntegerType()))
usersDF = usersDF.withColumn("productsSold", col("productsSold").cast(IntegerType()))
usersDF = usersDF.withColumn("productsWished", col("productsWished").cast(IntegerType()))
usersDF = usersDF.withColumn("productsBought", col("productsBought").cast(IntegerType()))

usersDF = usersDF.withColumn("productsPassRate", col("productsPassRate").cast(DecimalType(10, 2)))
usersDF = usersDF.withColumn("seniorityAsMonths", col("seniorityAsMonths").cast(DecimalType(10, 2)))
usersDF = usersDF.withColumn("seniorityAsYears", col("seniorityAsYears").cast(DecimalType(10, 2)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Deduplicate Users
# MAGIC
# MAGIC `identifierHash` should be unique per user. Drop exact duplicate identifiers, keeping the first occurrence.

# COMMAND ----------

pre_dedup_count = usersDF.count()

usersDF = usersDF.dropDuplicates(["identifierHash"])

post_dedup_count = usersDF.count()

print(f"Users before dedup: {pre_dedup_count}")
print(f"Users after dedup:  {post_dedup_count}")
print(f"Duplicates removed: {pre_dedup_count - post_dedup_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver Users Table
# MAGIC
# MAGIC Write the cleaned users dataframe as a managed Delta table in Unity Catalog.
# MAGIC
# MAGIC Write the cleaned users data as a managed Silver Delta table.

# COMMAND ----------

usersDF.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{silver_schema}.users")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {silver_schema}.users LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Buyers Silver Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cast Integer Columns

# COMMAND ----------

integer_columns = [
    'buyers', 'topbuyers', 'femalebuyers', 'malebuyers',
    'topfemalebuyers', 'topmalebuyers', 'totalproductsbought',
    'totalproductswished', 'totalproductsliked', 'toptotalproductsbought',
    'toptotalproductswished', 'toptotalproductsliked'
]

for column_name in integer_columns:
    buyersDF = buyersDF.withColumn(column_name, col(column_name).cast(IntegerType()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cast Decimal Columns

# COMMAND ----------

decimal_columns = [
    'topbuyerratio', 'femalebuyersratio', 'topfemalebuyersratio',
    'boughtperwishlistratio', 'boughtperlikeratio', 'topboughtperwishlistratio',
    'topboughtperlikeratio', 'meanproductsbought', 'meanproductswished',
    'meanproductsliked', 'topmeanproductsbought', 'topmeanproductswished',
    'topmeanproductsliked', 'meanofflinedays', 'topmeanofflinedays',
    'meanfollowers', 'meanfollowing', 'topmeanfollowers', 'topmeanfollowing'
]

for column_name in decimal_columns:
    buyersDF = buyersDF.withColumn(column_name, col(column_name).cast(DecimalType(10, 2)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standardize, Fill Nulls, and Derive Buyer Metrics

# COMMAND ----------

# Normalize country names
buyersDF = buyersDF.withColumn("country", initcap(col("country")))

for col_name in integer_columns:
    buyersDF = buyersDF.fillna({col_name: 0})

# Calculate the ratio of female to male buyers
buyersDF = buyersDF.withColumn(
    "female_to_male_ratio",
    round(col("femalebuyers") / (col("malebuyers") + 1), 2)
)

# Determine the market potential by comparing wishlist and purchases
buyersDF = buyersDF.withColumn(
    "wishlist_to_purchase_ratio",
    round(col("totalproductswished") / (col("totalproductsbought") + 1), 2)
)

# Tag countries with a high engagement ratio
high_engagement_threshold = 0.5
buyersDF = buyersDF.withColumn(
    "high_engagement",
    when(col("boughtperwishlistratio") > high_engagement_threshold, True).otherwise(False)
)

# Flag markets with increasing female buyer participation
buyersDF = buyersDF.withColumn(
    "growing_female_market",
    when(col("femalebuyersratio") > col("topfemalebuyersratio"), True).otherwise(False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplicate Buyers
# MAGIC
# MAGIC Each row represents one country's buyer aggregate, so `country` should be unique.

# COMMAND ----------

buyersDF = buyersDF.dropDuplicates(["country"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver Buyers Table

# COMMAND ----------

buyersDF.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{silver_schema}.buyers")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {silver_schema}.buyers LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Sellers Silver Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cast Columns to Correct Types

# COMMAND ----------

sellersDF = sellersDF \
    .withColumn("nbsellers", col("nbsellers").cast(IntegerType())) \
    .withColumn("meanproductssold", col("meanproductssold").cast(DecimalType(10, 2))) \
    .withColumn("meanproductslisted", col("meanproductslisted").cast(DecimalType(10, 2))) \
    .withColumn("meansellerpassrate", col("meansellerpassrate").cast(DecimalType(10, 2))) \
    .withColumn("totalproductssold", col("totalproductssold").cast(IntegerType())) \
    .withColumn("totalproductslisted", col("totalproductslisted").cast(IntegerType())) \
    .withColumn("meanproductsbought", col("meanproductsbought").cast(DecimalType(10, 2))) \
    .withColumn("meanproductswished", col("meanproductswished").cast(DecimalType(10, 2))) \
    .withColumn("meanproductsliked", col("meanproductsliked").cast(DecimalType(10, 2))) \
    .withColumn("totalbought", col("totalbought").cast(IntegerType())) \
    .withColumn("totalwished", col("totalwished").cast(IntegerType())) \
    .withColumn("totalproductsliked", col("totalproductsliked").cast(IntegerType())) \
    .withColumn("meanfollowers", col("meanfollowers").cast(DecimalType(10, 2))) \
    .withColumn("meanfollows", col("meanfollows").cast(DecimalType(10, 2))) \
    .withColumn("percentofappusers", col("percentofappusers").cast(DecimalType(10, 2))) \
    .withColumn("percentofiosusers", col("percentofiosusers").cast(DecimalType(10, 2))) \
    .withColumn("meanseniority", col("meanseniority").cast(DecimalType(10, 2)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standardize and Derive Seller Metrics

# COMMAND ----------

# Normalize country names and gender values
sellersDF = sellersDF.withColumn("country", initcap(col("country"))) \
                     .withColumn("sex", upper(col("sex")))

# Add a column to categorize the number of sellers
sellersDF = sellersDF.withColumn(
    "seller_size_category",
    when(col("nbsellers") < 500, "Small")
    .when((col("nbsellers") >= 500) & (col("nbsellers") < 2000), "Medium")
    .otherwise("Large")
)

# Calculate the mean products listed per seller as an indicator of seller activity
sellersDF = sellersDF.withColumn(
    "mean_products_listed_per_seller",
    round(col("totalproductslisted") / col("nbsellers"), 2)
)

# Identify markets with high seller pass rate
sellersDF = sellersDF.withColumn(
    "high_seller_pass_rate",
    when(col("meansellerpassrate") > 0.75, "High").otherwise("Normal")
)

# Fill nulls in meansellerpassrate with the overall average pass rate
mean_pass_rate = sellersDF.select(round(avg("meansellerpassrate"), 2).alias("avg_pass_rate")).collect()[0]["avg_pass_rate"]

sellersDF = sellersDF.withColumn(
    "meansellerpassrate",
    when(col("meansellerpassrate").isNull(), mean_pass_rate).otherwise(col("meansellerpassrate"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver Sellers Table

# COMMAND ----------

sellersDF.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{silver_schema}.sellers")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {silver_schema}.sellers LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Countries Silver Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cast Columns to Correct Types

# COMMAND ----------

countriesDF = countriesDF \
    .withColumn("sellers", col("sellers").cast(IntegerType())) \
    .withColumn("topsellers", col("topsellers").cast(IntegerType())) \
    .withColumn("topsellerratio", col("topsellerratio").cast(DecimalType(10, 2))) \
    .withColumn("femalesellersratio", col("femalesellersratio").cast(DecimalType(10, 2))) \
    .withColumn("topfemalesellersratio", col("topfemalesellersratio").cast(DecimalType(10, 2))) \
    .withColumn("femalesellers", col("femalesellers").cast(IntegerType())) \
    .withColumn("malesellers", col("malesellers").cast(IntegerType())) \
    .withColumn("topfemalesellers", col("topfemalesellers").cast(IntegerType())) \
    .withColumn("topmalesellers", col("topmalesellers").cast(IntegerType())) \
    .withColumn("countrysoldratio", col("countrysoldratio").cast(DecimalType(10, 2))) \
    .withColumn("bestsoldratio", col("bestsoldratio").cast(DecimalType(10, 2))) \
    .withColumn("toptotalproductssold", col("toptotalproductssold").cast(IntegerType())) \
    .withColumn("totalproductssold", col("totalproductssold").cast(IntegerType())) \
    .withColumn("toptotalproductslisted", col("toptotalproductslisted").cast(IntegerType())) \
    .withColumn("totalproductslisted", col("totalproductslisted").cast(IntegerType())) \
    .withColumn("topmeanproductssold", col("topmeanproductssold").cast(DecimalType(10, 2))) \
    .withColumn("topmeanproductslisted", col("topmeanproductslisted").cast(DecimalType(10, 2))) \
    .withColumn("meanproductssold", col("meanproductssold").cast(DecimalType(10, 2))) \
    .withColumn("meanproductslisted", col("meanproductslisted").cast(DecimalType(10, 2))) \
    .withColumn("meanofflinedays", col("meanofflinedays").cast(DecimalType(10, 2))) \
    .withColumn("topmeanofflinedays", col("topmeanofflinedays").cast(DecimalType(10, 2))) \
    .withColumn("meanfollowers", col("meanfollowers").cast(DecimalType(10, 2))) \
    .withColumn("meanfollowing", col("meanfollowing").cast(DecimalType(10, 2))) \
    .withColumn("topmeanfollowers", col("topmeanfollowers").cast(DecimalType(10, 2))) \
    .withColumn("topmeanfollowing", col("topmeanfollowing").cast(DecimalType(10, 2)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standardize and Derive Country Metrics

# COMMAND ----------

countriesDF = countriesDF.withColumn("country", initcap(col("country")))

# Calculating the ratio of top sellers to total sellers
countriesDF = countriesDF.withColumn(
    "top_seller_ratio",
    round(col("topsellers") / col("sellers"), 2)
)

# Flag countries with a high ratio of female sellers
countriesDF = countriesDF.withColumn(
    "high_female_seller_ratio",
    when(col("femalesellersratio") > 0.5, True).otherwise(False)
)

# Adding a performance indicator based on the sold/listed ratio
countriesDF = countriesDF.withColumn(
    "performance_indicator",
    round(col("toptotalproductssold") / (col("toptotalproductslisted") + 1), 2)
)

# Flag countries with exceptionally high performance
performance_threshold = 0.8
countriesDF = countriesDF.withColumn(
    "high_performance",
    when(col("performance_indicator") > performance_threshold, True).otherwise(False)
)

countriesDF = countriesDF.withColumn(
    "activity_level",
    when(col("meanofflinedays") < 30, "Highly Active")
    .when((col("meanofflinedays") >= 30) & (col("meanofflinedays") < 60), "Moderately Active")
    .otherwise("Low Activity")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver Countries Table

# COMMAND ----------

countriesDF.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{silver_schema}.countries")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {silver_schema}.countries LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Silver Row Counts
# MAGIC
# MAGIC Sanity check that every Silver table was created and populated in Unity Catalog.

# COMMAND ----------

for table_name in ["users", "buyers", "sellers", "countries"]:
    count_df = spark.sql(f"SELECT COUNT(*) AS row_count FROM {silver_schema}.{table_name}")
    print(table_name)
    display(count_df)
