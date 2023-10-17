import pandas as pd
unified_path = (r"C:\Users\oh255011\Documents\Teradata\SMX\UNIFIED\Unified.xlsx")


df = pd.read_excel(unified_path, 0)

print(df)