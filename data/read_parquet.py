import pyarrow.parquet as pq

table = pq.read_table("factures_raw.parquet")

df = table.to_pandas()

print(df)