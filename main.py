import pandas as pd
import time
import dask.dataframe as dd
import csv
from collections import defaultdict

def calculate_category_sums_csv(file_path):
    category_sums = defaultdict(float)

    with open(file_path, newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        
        for row in reader:
            category = row['category']
            amount = float(row['amount'])
            category_sums[category] += amount
    
    return category_sums

def run_python_csv(file_path):
    start_time = time.time()
    
    category_sums = calculate_category_sums_csv(file_path)
    
    for category, total in category_sums.items():
        print(f"{category}: {total:.2f}")
    
    print(f"Час виконання з Python: {time.time() - start_time} секунд\n")

def calculate_category_sums_dask(file_path):
    df = dd.read_csv(file_path)
    result = df.groupby('category')['amount'].sum().compute()
    return result

def run_dask(file_path):
    start_time = time.time()
    
    result = calculate_category_sums_dask(file_path)
    
    for category, total in result.items():
        print(f"{category}: {total:.2f}")
    
    print(f"Час виконання з Dask: {time.time() - start_time} секунд")

def main():
    file_path = "D:\\универ\\3 курс\\ПарП\\Завдання 2\\large_transactions.csv"
    
    print("Робота з Python:")
    run_python_csv(file_path)
    
    print("Робота з Dask:")
    run_dask(file_path)

if __name__ == "__main__":
    main()
