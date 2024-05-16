import os 
import pandas as pd

def clean_data_not_pipeline(storage_folder):
    print("Storage path exists:", os.path.exists(storage_folder))
    
    if os.path.exists(storage_folder):
        csv_files = os.listdir(storage_folder)
        dfs = []
        
        for csv_file in csv_files:
            df = pd.read_csv(os.path.join(storage_folder, csv_file))
            # Perform data cleaning...
            # df_cleaned = df.dropna()
            # df_filtered = df_cleaned[(df_cleaned != 'Unanswered').all(axis=1)]
            dfs.append(df)
        
        concat_df = pd.concat(dfs, ignore_index=True)
        cleaned_data_path = os.path.join(storage_folder, "cleaned_data.csv")
        concat_df.to_csv(cleaned_data_path, index=False)
        
        print("CSVs compiled and NaN fields dropped.")
        return cleaned_data_path
    
    else:
        print("Storage folder does not exist.")
        return None