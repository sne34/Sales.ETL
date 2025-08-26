import pandas as pd

def extract():
    # Use the correct full path
    df = pd.read_csv(r"C:\Users\sneha\OneDrive\Desktop\SALES-ETL\data\raw\sales_data_sample.csv", encoding="latin1")
    return df

if __name__ == "__main__":
    data = extract()
    print("✅ Data Extracted")
    print(data.head())

