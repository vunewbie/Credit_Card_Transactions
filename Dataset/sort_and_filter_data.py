import pandas as pd

# Đọc file CSV
df = pd.read_csv('Dataset/unsorted_data.csv')

# Sắp xếp theo Year, Month, Day, Time
df_sorted = df.sort_values(['Year', 'Month', 'Day', 'Time'])

df_filtered = df_sorted[df_sorted['Year'] >= 2000]

# Lưu file đã sắp xếp
df_filtered.to_csv('Dataset/credit_card_transactions.csv', index=False)