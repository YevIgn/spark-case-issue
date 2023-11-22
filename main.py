from pyspark.sql import SparkSession

test_schema = ["client_id", "sales_1y", "sales_2y", "sales_3y"]

test_data = [
    [1, 12, 4, 15],
    [2, 5, 3, 0],
    [3, 0, 0, 0],
]

# Different order of case summation.
sql_case_fails = """
    select
        client_id,
        case
            when sales_1y + sales_2y + sales_3y >= 10 then 'VIP'
            when sales_1y + sales_3y + sales_2y > 0 then 'REGULAR'
            else 'NONE'
        end as client_level
    from sales
"""


# Same order in case summation.
sql_case_succeeds = """
    select
        client_id,
        case
            when sales_1y + sales_2y + sales_3y >= 10 then 'VIP'
            when sales_1y + sales_2y + sales_3y > 0 then 'REGULAR'
            else 'NONE'
        end as client_level
    from sales
"""


if __name__ == "__main__":
    spark = SparkSession.builder.master("local[1]").getOrCreate()
    df = spark.createDataFrame(test_data, test_schema)
    df.createOrReplaceTempView("sales")

    # Displays client levels.
    spark.sql(sql_case_succeeds).show()

    # Errors out, using CTE for summation succeeds.
    spark.sql(sql_case_fails).show()
