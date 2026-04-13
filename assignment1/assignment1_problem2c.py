#!/usr/bin/env python3

import duckdb

DATASET = "/data/courses/2026_dat471_dit066/datasets/bike_sharing_hourly.csv"

con = duckdb.connect()

# Register the CSV file as a virtual table
con.execute(f"""
    CREATE VIEW bike_sharing AS
    SELECT * FROM read_csv_auto('{DATASET}');
""")

# 1. Number of rows
rows = con.execute("""
    SELECT COUNT(*)
    FROM bike_sharing;
""").fetchone()[0]

# 2. Average hourly bike rentals
avg_hourly = con.execute("""
    SELECT AVG(cnt)
    FROM bike_sharing;
""").fetchone()[0]

# 3. Top-5 busiest hours by average bike rentals
top5 = con.execute("""
    SELECT hr, AVG(cnt) AS avg_bike_rentals
    FROM bike_sharing
    GROUP BY hr
    ORDER BY avg_bike_rentals DESC, hr ASC
    LIMIT 5;
""").fetchall()

# 4. Average daily bike rentals in January 2012
avg_daily_jan_2012 = con.execute("""
    SELECT AVG(daily_total)
    FROM (
        SELECT dteday, SUM(cnt) AS daily_total
        FROM bike_sharing
        WHERE strftime(CAST(dteday AS DATE), '%Y-%m') = '2012-01'
        GROUP BY dteday
    ) AS january_2012_daily_totals;
""").fetchone()[0]

print(f"Number of rows: {rows}")
print(f"Average hourly bike rentals: {avg_hourly:.2f}")
print("Top-5 busiest hours by average bike rentals:")
for hr, avg in top5:
    print(f"  Hour {hr}: {avg:.2f}")
print(f"Average daily bike rentals in January 2012: {avg_daily_jan_2012:.2f}")
