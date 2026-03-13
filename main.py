from data_loader import load_csv

df = load_csv()

print("Total rows:", len(df))
print(df.head())
