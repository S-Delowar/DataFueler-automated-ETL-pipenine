import pandas as pd

df = pd.read_csv("validated_mongodb_data.csv")

print(df.shape)
print(df.head(3))