"""
Delaware North: Take-Home Assignment

Technical Prerequisites:
  - Python 3+
  - Spark: pip install delta-spark
  - Java Runtime: https://java.com/en/download/manual.jsp

Assignment Background:
    - You are a freelance analytics consultant who has partnered with the TTPD (Tiny Town Police Department)
      to analyze speeding tickets that have been given to the adult citizens of Tiny Town over the 2020-2023 period.
    - Inside the folder "ttpd_data" you will find a directory of data for Tiny Town. This dataset will need to be "ingested" for analysis.
    - The solutions must use the Dataframes API.
    - You will need to ingest this data into a PySpark environment and answer the following three questions for the TTPD.

Questions:
    1. Which police officer was handed the most speeding tickets?
        - Police officers are recorded as citizens. Find in the data what differentiates an officer from a non-officer.
    2. What 3 months (year + month) had the most speeding tickets? 
        - Bonus: What overall month-by-month or year-by-year trends, if any, do you see?
    3. Using the ticket fee table below, who are the top 10 people who have spent the most money paying speeding tickets overall?

Ticket Fee Table:
    - Ticket (base): $30
    - Ticket (base + school zone): $60
    - Ticket (base + construction work zone): $60
    - Ticket (base + school zone + construction work zone): $120
"""
from delta import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os


def prep_files(tables: list) -> None:
    """Prepares files in ttpd_data for querying by spark.

    Parameters:
        tables (list):The list of table names to be organized into directories/partitions.

    Returns:
        spark (SparkSession): the active Spark Session
    """
    data_folder = "ttpd_data"
    files = [f for f in os.listdir(data_folder) if "." in f]
    for table in tables:
        try:
            os.mkdir(f"ttpd_data/{table}")
        except FileExistsError:
            print(f"Directory '{table}' already exists.")

        for file in files:
            if table in file:
                os.rename(f"{data_folder}/{file}", f"{data_folder}/{table}/{file}")


def get_spark_session() -> SparkSession:
    """Retrieves or creates an active Spark Session for Delta operations

    Returns:
        spark (SparkSession): the active Spark Session
    """
    builder = (
        SparkSession.builder.appName("takehome")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()


def main():
    tables = ["automobiles", "people", "speeding_tickets"]
    prep_files(tables)
    spark: SparkSession = get_spark_session()

    # reading in people and tickets tables
    df_people = spark.read.option("delimiter", "|").csv(
        "ttpd_data/people/", header="true"
    )
    df_tickets = (
        spark.read.json("ttpd_data/speeding_tickets/*.json")
        .select("*", F.inline("speeding_tickets"))
        .drop("speeding_tickets")
    )

    # creating a column for fees
    df_tickets = df_tickets.withColumn(
        "fee", F.when(F.col("school_zone_ind") == True, 60).otherwise(30)
    ).withColumn(
        "fee",
        F.when(F.col("work_zone_ind") == True, F.col("fee") * 2).otherwise(
            F.col("fee")
        ),
    )

    # grouping officer_id with aggs of fees and count of tickets
    df_tickets_agg = df_tickets.groupBy("officer_id").agg(
        F.sum("fee").alias("total_fees"), F.count("officer_id").alias("total_tickets")
    )

    # joining officer names to agg ticket table
    df_most_tickets = df_tickets_agg.join(
        df_people, df_tickets_agg.officer_id == df_people.id, how="inner"
    ).select(
        F.col("total_fees"),
        F.col("total_tickets"),
        F.col("first_name"),
        F.col("last_name"),
    )

    # ordering tickets in desc order
    df_most_tickets = df_most_tickets.orderBy("total_tickets", ascending=False)

    # saving first answer here
    df_most_tickets.write.format("delta").mode("overwrite").save(
        "data/most_tickets_officers"
    )

    # get three month agg, start with getting quarters and years
    df_tickets = (
        df_tickets.withColumn("month", F.month(F.col("ticket_time")))
        .withColumn("quarter", F.quarter(F.col("ticket_time")))
        .withColumn("year", F.year(F.col("ticket_time")))
    )

    # group the aggs
    df_tickets_quarters = df_tickets.groupBy("quarter", "year").agg(
        F.sum("fee").alias("total_fees"), F.count("officer_id").alias("total_tickets")
    )
    df_tickets_months = df_tickets.groupBy("month", "year").agg(
        F.sum("fee").alias("total_fees"), F.count("officer_id").alias("total_tickets")
    )
    df_tickets_years = df_tickets.groupBy("year").agg(
        F.sum("fee").alias("total_fees"), F.count("officer_id").alias("total_tickets")
    )

    # order by most tickets
    df_tickets_quarters = df_tickets_quarters.orderBy("total_tickets", ascending=False)

    # order by time to see if theres any trends
    df_tickets_months = df_tickets_months.orderBy("year", "month", ascending=False)
    df_tickets_years = df_tickets_years.orderBy("year", ascending=False)

    # saving second answer here
    df_tickets_quarters.write.format("delta").mode("overwrite").save(
        "data/tickets_quarters"
    )
    df_tickets_months.write.format("delta").mode("overwrite").save(
        "data/tickets_months"
    )
    df_tickets_years.write.format("delta").mode("overwrite").save("data/tickets_years")

    # getting most spent by speeders
    df_speeders = df_tickets.groupBy("license_plate").agg(
        F.sum("fee").alias("total_fees"),
        F.count("license_plate").alias("total_tickets"),
    )
    df_speeders = df_speeders.orderBy("total_fees", ascending=False)

    # saving 3rd answer here
    df_speeders.write.format("delta").mode("overwrite").save("data/speeders")

    """
    df_most_tickets.show() # first answer
    df_tickets_quarters.show() # second answer
    df_tickets_months.show(df_tickets_months.count()) # second (bonus) answer
    df_tickets_years.show() # second (bonus) answer
    df_speeders.show() # third answer
    """


if __name__ == "__main__":
    main()
