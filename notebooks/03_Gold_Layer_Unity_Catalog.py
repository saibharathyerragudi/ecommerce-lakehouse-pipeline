# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold Layer with Unity Catalog
# MAGIC
# MAGIC This notebook reads the Silver Delta tables from Unity Catalog and builds one wide,
# MAGIC business-ready Gold table for SQL analytics and downstream reporting.
# MAGIC
# MAGIC Flow:
# MAGIC
# MAGIC `ecom_db_bharath.silver` tables -> country-level join -> `ecom_db_bharath.gold.ecom_one_big_table`
# MAGIC
# MAGIC Note: `users` is a user-level (row-per-user) table while `countries`, `buyers`, and
# MAGIC `sellers` are already country-level aggregates. To build one wide table we first
# MAGIC aggregate `users` up to the country level, then join all four on `country`.

# COMMAND ----------

silver_schema = "ecom_db_bharath.silver"
gold_schema = "ecom_db_bharath.gold"

print(silver_schema)
print(gold_schema)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver Tables

# COMMAND ----------

silver_users = spark.read.table(f"{silver_schema}.users")
silver_countries = spark.read.table(f"{silver_schema}.countries")
silver_buyers = spark.read.table(f"{silver_schema}.buyers")
silver_sellers = spark.read.table(f"{silver_schema}.sellers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate Users to Country Level
# MAGIC
# MAGIC `silver_users` is one row per user. Roll it up to one row per country so it can be
# MAGIC joined cleanly against the other three tables, which are already country-level.

# COMMAND ----------

users_by_country = silver_users.groupBy("country").agg(
    count("*").alias("Users_TotalUsers"),
    sum("productsSold").alias("Users_TotalProductsSold"),
    sum("productsWished").alias("Users_TotalProductsWished"),
    sum("productsBought").alias("Users_TotalProductsBought"),
    sum("productsListed").alias("Users_TotalProductsListed"),
    round(avg("productsPassRate"), 2).alias("Users_AvgProductsPassRate"),
    round(avg("account_age_years"), 2).alias("Users_AvgAccountAgeYears"),
    round(avg("socialNbFollowers"), 2).alias("Users_AvgSocialFollowers"),
    round(avg("socialNbFollows"), 2).alias("Users_AvgSocialFollows"),
    round(sum(when(col("hasAnyApp") == True, 1).otherwise(0)) / count("*") * 100, 2).alias("Users_PctWithAnyApp"),
    round(sum(when(col("gender") == "Female", 1).otherwise(0)) / count("*") * 100, 2).alias("Users_PctFemale"),
    round(sum(when(col("flag_long_title") == True, 1).otherwise(0)), 0).alias("Users_LongTitleFlagCount")
)

# COMMAND ----------

display(users_by_country.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join Country-Level Tables
# MAGIC
# MAGIC Outer join so a country present in any of the four sources is retained.
# MAGIC Note: the `users`/`countries`/`buyers`/`sellers` country name spellings must match
# MAGIC (e.g. `France`, `Germany`) for the join to align correctly — worth double-checking
# MAGIC with a row-count comparison after this step if numbers look off.

# COMMAND ----------

comprehensive_country_table = users_by_country \
    .join(silver_countries, ["country"], "outer") \
    .join(silver_buyers, ["country"], "outer") \
    .join(silver_sellers, ["country"], "outer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select and Alias Final Gold Columns
# MAGIC
# MAGIC Every column carried into the Gold table is explicitly aliased with a source prefix
# MAGIC so downstream BI tools show clear, unambiguous field names.

# COMMAND ----------

comprehensive_country_table = comprehensive_country_table.select(
    col("country").alias("Country"),

    # From users_by_country (aggregated from silver_users)
    col("Users_TotalUsers"),
    col("Users_TotalProductsSold"),
    col("Users_TotalProductsWished"),
    col("Users_TotalProductsBought"),
    col("Users_TotalProductsListed"),
    col("Users_AvgProductsPassRate"),
    col("Users_AvgAccountAgeYears"),
    col("Users_AvgSocialFollowers"),
    col("Users_AvgSocialFollows"),
    col("Users_PctWithAnyApp"),
    col("Users_PctFemale"),
    col("Users_LongTitleFlagCount"),

    # From silver_countries
    silver_countries["sellers"].alias("Countries_Sellers"),
    silver_countries["topsellers"].alias("Countries_TopSellers"),
    silver_countries["femalesellers"].alias("Countries_FemaleSellers"),
    silver_countries["malesellers"].alias("Countries_MaleSellers"),
    silver_countries["topfemalesellers"].alias("Countries_TopFemaleSellers"),
    silver_countries["topmalesellers"].alias("Countries_TopMaleSellers"),
    silver_countries["top_seller_ratio"].alias("Countries_TopSellerRatio"),
    silver_countries["high_female_seller_ratio"].alias("Countries_HighFemaleSellerRatio"),
    silver_countries["performance_indicator"].alias("Countries_PerformanceIndicator"),
    silver_countries["high_performance"].alias("Countries_HighPerformance"),
    silver_countries["activity_level"].alias("Countries_ActivityLevel"),
    silver_countries["totalproductssold"].alias("Countries_TotalProductsSold"),
    silver_countries["totalproductslisted"].alias("Countries_TotalProductsListed"),

    # From silver_buyers
    silver_buyers["buyers"].alias("Buyers_Total"),
    silver_buyers["topbuyers"].alias("Buyers_Top"),
    silver_buyers["femalebuyers"].alias("Buyers_Female"),
    silver_buyers["malebuyers"].alias("Buyers_Male"),
    silver_buyers["topfemalebuyers"].alias("Buyers_TopFemale"),
    silver_buyers["topmalebuyers"].alias("Buyers_TopMale"),
    silver_buyers["female_to_male_ratio"].alias("Buyers_FemaleToMaleRatio"),
    silver_buyers["wishlist_to_purchase_ratio"].alias("Buyers_WishlistToPurchaseRatio"),
    silver_buyers["high_engagement"].alias("Buyers_HighEngagement"),
    silver_buyers["growing_female_market"].alias("Buyers_GrowingFemaleMarket"),
    silver_buyers["totalproductsbought"].alias("Buyers_TotalProductsBought"),
    silver_buyers["totalproductswished"].alias("Buyers_TotalProductsWished"),
    silver_buyers["totalproductsliked"].alias("Buyers_TotalProductsLiked"),

    # From silver_sellers
    silver_sellers["nbsellers"].alias("Sellers_Total"),
    silver_sellers["sex"].alias("Sellers_Sex"),
    silver_sellers["meanproductssold"].alias("Sellers_MeanProductsSold"),
    silver_sellers["meanproductslisted"].alias("Sellers_MeanProductsListed"),
    silver_sellers["meansellerpassrate"].alias("Sellers_MeanPassRate"),
    silver_sellers["seller_size_category"].alias("Sellers_SizeCategory"),
    silver_sellers["mean_products_listed_per_seller"].alias("Sellers_MeanProductsListedPerSeller"),
    silver_sellers["high_seller_pass_rate"].alias("Sellers_HighPassRateFlag"),
    silver_sellers["totalproductssold"].alias("Sellers_TotalProductsSold"),
    silver_sellers["totalproductslisted"].alias("Sellers_TotalProductsListed"),
    silver_sellers["percentofappusers"].alias("Sellers_PercentAppUsers"),
    silver_sellers["percentofiosusers"].alias("Sellers_PercentIosUsers"),
)

# COMMAND ----------

display(comprehensive_country_table.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fill Nulls from the Outer Join
# MAGIC
# MAGIC An outer join across four sources will leave nulls where a country was missing from
# MAGIC one side. Fill numeric metric nulls with 0 so downstream sums/averages in BI tools
# MAGIC don't silently drop rows.

# COMMAND ----------

numeric_gold_columns = [
    field.name for field in comprehensive_country_table.schema.fields
    if isinstance(field.dataType, (IntegerType, LongType, DoubleType, DecimalType))
    and field.name != "Country"
]

comprehensive_country_table = comprehensive_country_table.fillna(0, subset=numeric_gold_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row Count Check
# MAGIC
# MAGIC Compare the Gold row count to distinct country counts across sources to sanity-check
# MAGIC the outer joins didn't unexpectedly fan out or drop rows.

# COMMAND ----------

print("Gold table rows:", comprehensive_country_table.count())
print("Distinct countries (users):", users_by_country.select("country").distinct().count())
print("Distinct countries (countries):", silver_countries.select("country").distinct().count())
print("Distinct countries (buyers):", silver_buyers.select("country").distinct().count())
print("Distinct countries (sellers):", silver_sellers.select("country").distinct().count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Gold Table
# MAGIC
# MAGIC Write the final wide, business-ready table as a managed Delta table in Unity Catalog.
# MAGIC
# MAGIC Save the final country-level table as a managed Gold Delta table.

# COMMAND ----------

comprehensive_country_table.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{gold_schema}.ecom_one_big_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Gold Table

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {gold_schema}.ecom_one_big_table ORDER BY Country LIMIT 20"))

# COMMAND ----------

display(spark.sql(f"SELECT COUNT(*) AS row_count FROM {gold_schema}.ecom_one_big_table"))
