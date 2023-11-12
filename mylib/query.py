"""
query and viz file
"""

from pyspark.sql import SparkSession
import matplotlib.pyplot as plt


# sample query
def query_transform():
    """
    Run a predefined SQL query on a Spark DataFrame.

    Returns:
        DataFrame: Result of the SQL query.
    """
    spark = SparkSession.builder.appName("Query").getOrCreate()
    query = ("""
    SELECT group, count(*) AS cnt
    FROM daily_show 
    WHERE year=1999
    GROUP BY group
    ORDER BY cnt desc
""")
    query_result = spark.sql(query)
    return query_result


# sample viz for project
def viz():
    query = query_transform()
    count = query.count()
    if count > 0:
        print(f"Data validation passed. {count} rows available.")
    else:
        print("No data available. Please investigate.")

    pandas_df = query.select("group", "cnt").toPandas()

    # Plot a bar plot
    plt.figure(figsize=(15, 8))
    plt.bar(pandas_df["group"], pandas_df["cnt"], color='skyblue')
    plt.title("Number of different show in 1999")
    plt.xlabel("group")
    plt.ylabel("cnt")
    plt.show()
    

if __name__ == "__main__":
    query_transform()
    viz()