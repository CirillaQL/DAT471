import time
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, coalesce, datediff, lit, to_date, udf, col, when, year
from pyspark.sql.types import IntegerType
from pyspark import StorageLevel
import pandas as pd
import sys

@udf(returnType=IntegerType())
def jdn(dt):
    """
    Computes the Julian date number for a given date.
    Parameters:
    - dt, datetime : the Gregorian date for which to compute the number

    Return value: an integer denoting the number of days since January 1, 
    4714 BC in the proleptic Julian calendar.
    """
    y = dt.year
    m = dt.month
    d = dt.day
    if m < 3:
        y -= 1
        m += 12
    a = y//100
    b = a//4
    c = 2-a+b
    e = int(365.25*(y+4716))
    f = int(30.6001*(m+1))
    jd = c+d+e+f-1524
    return jd

    
# you probably want to use a function with this signature for computing the
# simple linear regression with least squares using applyInPandas()
# key is the group key, df is a Pandas dataframe
# should return a Pandas dataframe
def lsq(key, df):
    def get_column(*names):
        columns = {column.lower(): column for column in df.columns}
        for name in names:
            column = columns.get(name.lower())
            if column is not None:
                return column
        raise KeyError(f"None of these columns were found: {names}")

    station_column = get_column("STATION", "STATIONCODE", "station")
    name_column = get_column("NAME", "STATIONNAME", "station_name")
    jdn_column = get_column("JDN", "jdn")
    tavg_column = get_column("TAVG", "tavg")

    clean_df = df[[station_column, name_column, jdn_column, tavg_column]].dropna()
    if len(clean_df) < 2:
        return pd.DataFrame(columns=["STATION", "NAME", "BETA"])

    x = clean_df[jdn_column].astype(float)
    y = clean_df[tavg_column].astype(float)

    x_centered = x - x.mean()
    y_centered = y - y.mean()
    denominator = (x_centered * x_centered).sum()

    if denominator == 0:
        return pd.DataFrame(columns=["STATION", "NAME", "BETA"])

    beta = (x_centered * y_centered).sum() / denominator

    return pd.DataFrame(
        {
            "STATION": [clean_df[station_column].iloc[0]],
            "NAME": [clean_df[name_column].iloc[0]],
            "BETA": [beta],
        }
    )


if __name__ == '__main__':
    # do not change the interface
    parser = argparse.ArgumentParser(description = \
                                    'Compute climate data.')
    parser.add_argument('-w','--num-workers',default=1,type=int,
                            help = 'Number of workers')
    parser.add_argument('filename',type=str,help='Input filename')
    args = parser.parse_args()

    start = time.time()

    # this bit is important: by default, Spark only allocates 1 GiB of memory 
    # which will likely cause an out of memory exception with the full data
    spark = SparkSession.builder \
            .master(f'local[{args.num_workers}]') \
            .config("spark.driver.memory", "32g") \
            .config("spark.sql.shuffle.partitions", str(max(args.num_workers * 4, 64))) \
            .getOrCreate()
    
    # read the CSV file into a pyspark.sql dataframe and compute the things you need
    read_start = time.time()
    df = spark.read.csv(args.filename, header=True, inferSchema=False)
    df = df.select("STATION", "NAME", "DATE", "TMAX", "TMIN")
    df = df.withColumn(
        "DATE",
        coalesce(
            to_date(col("DATE")),
            to_date(col("DATE").cast("string"), "yyyyMMdd"),
        ),
    )
    df = df.withColumn("JDN", datediff(col("DATE"), lit("1970-01-01")) + lit(2440588))
    df = df.withColumn("TAVG", (col("TMIN") + col("TMAX")) / 2)
    df = df.withColumn("YEAR", year(col("DATE")))
    df = df.select("STATION", "NAME", "DATE", "JDN", "TAVG", "YEAR") \
        .filter(
            col("STATION").isNotNull()
            & col("NAME").isNotNull()
            & col("DATE").isNotNull()
            & col("JDN").isNotNull()
            & col("TAVG").isNotNull()
            & col("YEAR").isNotNull()
        ) \
        .persist(StorageLevel.DISK_ONLY)
    record_count = df.count()
    read_end = time.time()

    compute_start = time.time()
    slopes_schema = "STATION string, NAME string, BETA double"
    slopes = df.groupBy("STATION", "NAME") \
        .applyInPandas(lsq, schema=slopes_schema) \
        .cache()
    top5_slopes = slopes.orderBy(col("BETA").desc()).take(5)
    slope_count = slopes.count()
    positive_beta_fraction = slopes.filter(col("BETA") > 0).count() / slope_count
    beta_min = slopes.selectExpr("min(BETA) AS beta_min").first().beta_min
    beta_q1, beta_median, beta_q3 = slopes.approxQuantile(
        "BETA", [0.25, 0.5, 0.75], 0.001
    )
    beta_max = slopes.selectExpr("max(BETA) AS beta_max").first().beta_max

    decade_df = df.withColumn(
        "DECADE",
        when((col("YEAR") >= 1910) & (col("YEAR") <= 1919), lit("1910s"))
        .when((col("YEAR") >= 2010) & (col("YEAR") <= 2019), lit("2010s"))
    ).filter(col("DECADE").isNotNull())

    decade_averages = decade_df.groupBy("STATION", "NAME", "DECADE") \
        .agg(avg("TAVG").alias("TAVG_DECADE"))

    averages_1910s = decade_averages.filter(col("DECADE") == "1910s") \
        .select("STATION", "NAME", col("TAVG_DECADE").alias("TAVG_1910s"))
    averages_2010s = decade_averages.filter(col("DECADE") == "2010s") \
        .select("STATION", "NAME", col("TAVG_DECADE").alias("TAVG_2010s"))

    tavg_differences = averages_1910s.join(
        averages_2010s, ["STATION", "NAME"], "inner"
    ).withColumn(
        "TAVGDIFF",
        (col("TAVG_2010s") - col("TAVG_1910s")) * (5 / 9),
    ).cache()

    tdiff_count = tavg_differences.count()
    top5_differences = tavg_differences.orderBy(col("TAVGDIFF").desc()).take(5)

    if tdiff_count > 0:
        positive_tdiff_fraction = (
            tavg_differences.filter(col("TAVGDIFF") > 0).count() / tdiff_count
        )
        tdiff_min = tavg_differences.selectExpr(
            "min(TAVGDIFF) AS tdiff_min"
        ).first().tdiff_min
        tdiff_q1, tdiff_median, tdiff_q3 = tavg_differences.approxQuantile(
            "TAVGDIFF", [0.25, 0.5, 0.75], 0.001
        )
        tdiff_max = tavg_differences.selectExpr(
            "max(TAVGDIFF) AS tdiff_max"
        ).first().tdiff_max
    else:
        positive_tdiff_fraction = 0.0
        tdiff_min, tdiff_q1, tdiff_median, tdiff_q3, tdiff_max = 5 * [0.0]
    compute_end = time.time()

    print('Top 5 coefficients table:')
    print('| rank | station | name | beta (°F/day) |')
    print('| ---: | --- | --- | ---: |')
    for rank, row in enumerate(top5_slopes, start=1):
        print(f'| {rank} | {row.STATION} | {row.NAME} | {row.BETA:0.6e} |')

    print('BETA five-number summary table:')
    print('| statistic | value (°F/day) |')
    print('| --- | ---: |')
    print(f'| min | {beta_min:0.6e} |')
    print(f'| Q1 | {beta_q1:0.6e} |')
    print(f'| median | {beta_median:0.6e} |')
    print(f'| Q3 | {beta_q3:0.6e} |')
    print(f'| max | {beta_max:0.6e} |')

    print('Top 5 decade temperature differences table:')
    print('| rank | station | name | difference (°C) |')
    print('| ---: | --- | --- | ---: |')
    if tdiff_count == 0:
        print('| - | no matching stations | - | - |')
    else:
        for rank, row in enumerate(top5_differences, start=1):
            print(f'| {rank} | {row.STATION} | {row.NAME} | {row.TAVGDIFF:0.3f} |')

    print('Decade temperature difference five-number summary table:')
    print('| statistic | value (°C) |')
    print('| --- | ---: |')
    print(f'| min | {tdiff_min:0.3f} |')
    print(f'| Q1 | {tdiff_q1:0.3f} |')
    print(f'| median | {tdiff_median:0.3f} |')
    print(f'| Q3 | {tdiff_q3:0.3f} |')
    print(f'| max | {tdiff_max:0.3f} |')

    # top 5 slopes are printed here
    # replace None with your dataframe, list, or an appropriate expression
    # replace STATIONCODE, STATIONNAME, and BETA with appropriate expressions
    print('Top 5 coefficients:')
    for row in top5_slopes:
        print(f'{row.STATION} at {row.NAME} BETA={row.BETA:0.3e} °F/d')

    # replace None with an appropriate expression
    print('Fraction of positive coefficients:')
    print(positive_beta_fraction)

    # Five-number summary of slopes, replace with appropriate expressions
    print('Five-number summary of BETA values:')
    print(f'beta_min {beta_min:0.3e}')
    print(f'beta_q1 {beta_q1:0.3e}')
    print(f'beta_median {beta_median:0.3e}')
    print(f'beta_q3 {beta_q3:0.3e}')
    print(f'beta_max {beta_max:0.3e}')

    # Here you will need to implement computing the decadewise differences 
    # between the average temperatures of 1910s and 2010s

    # There should probably be an if statement to check if any such values were 
    # computed (no suitable stations in the tiny dataset!)

    # Note that values should be printed in celsius

    # Replace None with an appropriate expression
    # Replace STATION, STATIONNAME, and TAVGDIFF with appropriate expressions

    print('Top 5 differences:')
    if tdiff_count == 0:
        print('No stations have measurements from both the 1910s and 2010s.')
    else:
        for row in top5_differences:
            print(f'{row.STATION} at {row.NAME} difference {row.TAVGDIFF:0.1f} °C)')

    # replace None with an appropriate expression
    print('Fraction of positive differences:')
    print(positive_tdiff_fraction)

    # Five-number summary of temperature differences, replace with appropriate expressions
    print('Five-number summary of decade average difference values:')
    print(f'tdiff_min {tdiff_min:0.1f} °C')
    print(f'tdiff_q1 {tdiff_q1:0.1f} °C')
    print(f'tdiff_median {tdiff_median:0.1f} °C')
    print(f'tdiff_q3 {tdiff_q3:0.1f} °C')
    print(f'tdiff_max {tdiff_max:0.1f} °C')

    # Add your time measurements here
    # It may be interesting to also record more fine-grained times (e.g., how 
    # much time was spent computing vs. reading data)
    total_time = time.time() - start
    read_time = read_end - read_start
    compute_time = compute_end - compute_start
    measured_time = read_time + compute_time
    read_fraction = read_time / measured_time if measured_time > 0 else 0.0
    compute_fraction = compute_time / measured_time if measured_time > 0 else 0.0
    print(f'num workers: {args.num_workers}')
    print(f'records: {record_count}')
    print(f'read time: {read_time:0.6f} s')
    print(f'compute time: {compute_time:0.6f} s')
    print(f'read fraction: {read_fraction:0.6f}')
    print(f'compute fraction: {compute_fraction:0.6f}')
    print(f'total time: {total_time:0.6f} s')
