import pyarrow.parquet as pq

table = pq.read_table("factures_clean.parquet")

df = table.to_pandas()

print(df)