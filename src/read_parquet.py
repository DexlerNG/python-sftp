import pandas as pd

df = pd.read_parquet('./coupons/COUPON_20240330.parquet')
num_rows = df.shape[0]

print(f'Number of records: {num_rows}')
print(df.loc[0:])
