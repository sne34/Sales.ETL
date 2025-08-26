import pandas as pd

def transform():
    
    df = pd.read_csv(r"C:\Users\sneha\OneDrive\Desktop\SALES-ETL\data\raw\sales_data_sample.csv", encoding="latin1")
    
    #  Handle missing values
    
    df['TERRITORY'] = df['TERRITORY'].fillna('Unknown')
    
    #  Correct data types
    df['ORDERNUMBER'] = df['ORDERNUMBER'].astype(str)
    df['QUANTITYORDERED'] = df['QUANTITYORDERED'].astype(int)
    df['PRICEEACH'] = df['PRICEEACH'].astype(float)
    

    # Total price for each line
    df['TOTALPRICE'] = df['QUANTITYORDERED'] * df['PRICEEACH']
    
    #  Standardize text columns
    df['DEALSIZE'] = df['DEALSIZE'].str.capitalize()
    
    #  Droping unnecessary columns if any 
    # df = df.drop(columns=['CONTACTFIRSTNAME'])
    
    # Save the transformed data to interim folder
    df.to_csv(r"C:\Users\sneha\OneDrive\Desktop\Sales-ETL\data\interim\transformed_sales.csv", index=False)
    
    return df

if __name__ == "__main__":
    transformed_data = transform()
    print("✅ Data Transformed")
    print(transformed_data.head())
