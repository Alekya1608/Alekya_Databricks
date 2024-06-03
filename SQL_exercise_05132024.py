# Databricks notebook source
# boilerplate

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

# create spark session
# /Workspace/Users/datakraft867@gmail.com/books.csv
spark = SparkSession.builder.appName("la_crime_query").getOrCreate()
la_df = spark.read.csv("/Volumes/datakraft_batch1/default/datasets/la_crime_data.csv", header=True, inferSchema=True)

# COMMAND ----------

la_df.count()

# COMMAND ----------

la_df.createOrReplaceTempView("la_tbl")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM la_tbl limit 10;

# COMMAND ----------

la_df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM la_tbl;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(DISTINCT DR_NO) as cnt FROM la_tbl;

# COMMAND ----------

# Reviewing the questions above, I don't need all the columns in the dataset but only a few. Hence I'm creating a new data set with only the relevant columns and adding columns for year, month, date_reported based on Date Rptd. Columns I'm including - 
# date_reported (based on Date Rptd)
# DR_NO, renamed as crime_rpt_no. This is the primary key for the dataset, see above analysis to confirm. 
# Crm Cd Desc, renamed as crime_desc
# month, based on Date Rptd
# Season, based on month

# new dataset 
spark.sql("""
          
          CREATE OR REPLACE TEMP VIEW la_crime_vw AS
          WITH CTE1 AS (
          SELECT 
           to_date(LEFT(`Date Rptd`, 10), 'MM/dd/yyyy') AS date_reported,
           month(to_date(LEFT(`Date Rptd`, 10), 'MM/dd/yyyy')) AS crime_month,
           year(to_date(LEFT(`Date Rptd`, 10), 'MM/dd/yyyy')) AS crime_year,
           DR_NO AS crime_rpt_no,
           `Crm Cd Desc` AS crime_desc,
           `Vict Age` AS vict_age
          FROM  la_tbl
          )
          SELECT 
          *,
          CASE 
          WHEN crime_month BETWEEN 1 and 3 THEN 'winter'
          WHEN crime_month BETWEEN 4 and 6 THEN 'spring'
          WHEN crime_month BETWEEN 7 and 9 THEN 'summer'
          ELSE 'fall' END AS season
          FROM CTE1
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. What is the date range reported in this data set? 
# MAGIC
# MAGIC select
# MAGIC min(crime_year) as earliest_crime_year,
# MAGIC max(crime_year) as latest_crime_year
# MAGIC from la_crime_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC --total crimes according to years
# MAGIC
# MAGIC select crime_year,count(`crime_desc`)
# MAGIC from la_crime_vw
# MAGIC group by crime_year
# MAGIC order by crime_year
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2. Which crime saw the most increase in 2021 compared to 2020? 
# MAGIC -- Expected columns - year, crime description, crime reported in 2020, crime reported in 2021, increase in crimes
# MAGIC -- Additional detail: Dataset reports different types of crimes each year. I want to find out which crime saw the most increase from year Covid start year of 2020 to 2021?
# MAGIC
# MAGIC
# MAGIC
# MAGIC WITH CTE1 AS (
# MAGIC   SELECT crime_year, crime_desc AS crime_description, COUNT(crime_rpt_no) AS crime_cnt_2020
# MAGIC   FROM la_crime_vw
# MAGIC   WHERE crime_year = 2020
# MAGIC   GROUP BY crime_year, crime_desc
# MAGIC ),
# MAGIC CTE2 AS (
# MAGIC   SELECT crime_year, crime_desc AS crime_description, COUNT(crime_rpt_no) AS crime_cnt_2021
# MAGIC   FROM la_crime_vw
# MAGIC   WHERE crime_year = 2021
# MAGIC   GROUP BY crime_year, crime_desc
# MAGIC )
# MAGIC SELECT CTE1.crime_description, CTE1.crime_cnt_2020, CTE2.crime_cnt_2021, 
# MAGIC (CTE2.crime_cnt_2021 - CTE1.crime_cnt_2020) AS increase_in_crimes
# MAGIC FROM CTE1 
# MAGIC JOIN CTE2 ON CTE1.crime_description = CTE2.crime_description
# MAGIC ORDER BY increase_in_crimes DESC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3. For above question, would your answer change if the increase in crime in reported in % increase in crimes vs. total increase in crimes (i.e. just number)?
# MAGIC -- Expected columns - year, crime description, crime reported in 2021, crime reported in 2022, % increase in crimes
# MAGIC
# MAGIC WITH CTE1 AS (
# MAGIC   SELECT crime_year, crime_desc AS crime_description, COUNT(crime_rpt_no) AS crime_cnt_2020
# MAGIC   FROM la_crime_vw
# MAGIC   WHERE crime_year = 2020
# MAGIC   GROUP BY crime_year, crime_desc
# MAGIC ),
# MAGIC CTE2 AS (
# MAGIC   SELECT crime_year, crime_desc AS crime_description, COUNT(crime_rpt_no) AS crime_cnt_2021
# MAGIC   FROM la_crime_vw
# MAGIC   WHERE crime_year = 2021
# MAGIC   GROUP BY crime_year, crime_desc
# MAGIC )
# MAGIC SELECT DISTINCT 
# MAGIC   CTE1.crime_description, 
# MAGIC   CTE1.crime_cnt_2020, 
# MAGIC   CTE2.crime_cnt_2021, 
# MAGIC   ROUND((((CTE2.crime_cnt_2021 - CTE1.crime_cnt_2020) / CTE1.crime_cnt_2020) * 100), 2) AS percentage_increase_in_crimes
# MAGIC FROM CTE1 
# MAGIC JOIN CTE2 ON CTE1.crime_description = CTE2.crime_description
# MAGIC GROUP BY CTE1.crime_description, CTE1.crime_cnt_2020, CTE2.crime_cnt_2021

# COMMAND ----------

# MAGIC %sql
# MAGIC /* 4. Imagine a year has four seasons - 
# MAGIC   > Winter: Jan - Mar
# MAGIC   > Spring: Apr - Jun 
# MAGIC   > Summer: Jul - Sep 
# MAGIC   > Fall: Oct - Dec
# MAGIC For the year 2022, what are the top three crime by each season?
# MAGIC
# MAGIC Expected columns - Year, Season (i.e Winter, Spring), Crime description, crime count, Rank of crime (in top 3)
# MAGIC
# MAGIC # Sample output (not real data) - 
# MAGIC # 2022    Winter  Theft 2000 1
# MAGIC # 2022    Winter  Break-in 1000 2
# MAGIC # 2022    Winter  Car Jacking 500 3
# MAGIC # ....remaining 3 seasons....... */
# MAGIC
# MAGIC
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC   SELECT 
# MAGIC   crime_year,
# MAGIC   CASE
# MAGIC     WHEN crime_month BETWEEN 1 AND 3 THEN 'Winter'
# MAGIC     WHEN crime_month BETWEEN 4 AND 6 THEN 'Spring'
# MAGIC     WHEN crime_month BETWEEN 7 AND 9 THEN 'Summer'
# MAGIC     ELSE 'Fall'
# MAGIC   END AS season,
# MAGIC   crime_desc,
# MAGIC   count(crime_rpt_no) as crime_count,
# MAGIC   rank() OVER (PARTITION BY crime_year, 
# MAGIC   CASE
# MAGIC     WHEN crime_month BETWEEN 1 AND 3 THEN 'Winter'
# MAGIC     WHEN crime_month BETWEEN 4 AND 6 THEN 'Spring'
# MAGIC     WHEN crime_month BETWEEN 7 AND 9 THEN 'Summer'
# MAGIC     ELSE 'Fall'
# MAGIC   END ORDER BY count(crime_rpt_no) DESC) as crime_rank
# MAGIC FROM la_crime_vw
# MAGIC WHERE crime_year = 2022
# MAGIC GROUP BY crime_year,
# MAGIC   CASE
# MAGIC     WHEN crime_month BETWEEN 1 AND 3 THEN 'Winter'
# MAGIC     WHEN crime_month BETWEEN 4 AND 6 THEN 'Spring'
# MAGIC     WHEN crime_month BETWEEN 7 AND 9 THEN 'Summer'
# MAGIC     ELSE 'Fall'
# MAGIC   END,
# MAGIC   crime_desc
# MAGIC )
# MAGIC   WHERE crime_rank <= 3  
# MAGIC      
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC 5. What are the top three crimes faced by this age bracket in 2022: 20-29; 30-39; 40-49; 50-59 (the age of the perpetrator is within that age bracket)
# MAGIC
# MAGIC Expected columns - Year, Age Bracket, Crime Description, crime count, Rank of crime
# MAGIC
# MAGIC Sample output (not real data) - 
# MAGIC 2022 20-29    Theft   2000 1
# MAGIC 2022 20-29    Break-in   1000 2
# MAGIC 2022 20-29    Car Jacking   500 3
# MAGIC ....remaining age brackets ....... */
# MAGIC
# MAGIC select * 
# MAGIC from (
# MAGIC   SELECT crime_year, 
# MAGIC CASE
# MAGIC     WHEN vict_age BETWEEN 20 AND 29 THEN '20-29'
# MAGIC     WHEN vict_age BETWEEN 30 AND 39 THEN '30-39'
# MAGIC     WHEN vict_age BETWEEN 40 AND 49 THEN '40-49'
# MAGIC     WHEN vict_age BETWEEN 50 AND 59 THEN '50-59'
# MAGIC   END AS age_bracket,
# MAGIC crime_desc, count(crime_rpt_no) AS crime_count,
# MAGIC RANK() OVER (PARTITION BY crime_year, 
# MAGIC CASE
# MAGIC     WHEN vict_age BETWEEN 20 AND 29 THEN '20-29'
# MAGIC     WHEN vict_age BETWEEN 30 AND 39 THEN '30-39'
# MAGIC     WHEN vict_age BETWEEN 40 AND 49 THEN '40-49'
# MAGIC     WHEN vict_age BETWEEN 50 AND 59 THEN '50-59'
# MAGIC  END ORDER BY COUNT(crime_rpt_no) DESC) AS crime_rank
# MAGIC FROM la_crime_VW
# MAGIC where crime_year = 2022 and (vict_age BETWEEN 20 AND 29 OR VICT_AGE BETWEEN 30 AND 39 OR VICT_AGE BETWEEN 40 AND 49 OR VICT_AGE BETWEEN 50 AND 59)
# MAGIC group by CRIME_YEAR,
# MAGIC case
# MAGIC     WHEN vict_age BETWEEN 20 AND 29 THEN '20-29'
# MAGIC     WHEN vict_age BETWEEN 30 AND 39 THEN '30-39'
# MAGIC     WHEN vict_age BETWEEN 40 AND 49 THEN '40-49'
# MAGIC     WHEN vict_age BETWEEN 50 AND 59 THEN '50-59'
# MAGIC   END, CRIME_DESC)
# MAGIC   WHERE CRIME_RANK <= 3  
# MAGIC  
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC /* 6. Show the top three crimes that occured on the street for years 2022-2023? (Hint: Show top three crimes by year). 
# MAGIC
# MAGIC Expected columns - Year, Crime Description, crime count, Rank of crime
# MAGIC
# MAGIC Sample output (not real data) - 
# MAGIC 2022 Auto Theft 1000 1
# MAGIC 2022 Retail Theft 500 2
# MAGIC 2022 Break-in 100 3 */
# MAGIC
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC   SELECT CRIME_YEAR, CRIME_DESC, count(CRIME_RPT_NO) AS CRIME_COUNT,
# MAGIC   rank() OVER (PARTITION BY CRIME_YEAR ORDER BY count(CRIME_RPT_NO) DESC) AS RANK_CRIME
# MAGIC   FROM LA_CRIME_VW
# MAGIC   WHERE CRIME_YEAR BETWEEN 2022 AND 2023
# MAGIC   GROUP BY CRIME_YEAR, CRIME_DESC
# MAGIC   ORDER BY CRIME_COUNT DESC
# MAGIC )
# MAGIC WHERE RANK_CRIME <=3
