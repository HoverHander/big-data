from __future__ import annotations

import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import is_venv_installed

import requests
import pandas as pd

log = logging.getLogger(__name__)

if not is_venv_installed():
    log.warning("The tutorial_taskflow_api_virtualenv example DAG requires virtualenv, please install it.")
else:
    @dag(schedule=None, 
         start_date=datetime(2021, 1, 1), 
         catchup=False, 
         tags=['big_data', 'final_project'])

    def PPP_fraud_detection():
        """
        ### Pipeline for PPP fraud detection

        This pipeline acquires, cleans, analyzes, and visualizes the data from the Small Business Administration
        """

        
        @task.virtualenv(use_dill=True,
            system_site_packages=False,
            requirements=["pandas"])
        def acquire_data(urls):
            import os
            import http.client
            
            storage_folder = "/opt/airflow/storage/ppp"
            #if storage folder exists cd into it
            if os.path.exists(storage_folder):
                os.chdir(storage_folder)
           
            for idx, url in enumerate(urls):
                filename = f"csv_{idx}.csv" 
                if os.path.exists(filename):
                    print(f"CSV file {filename} already exists. Skipping download.")
                    # saved_csvs.append(filename)
                    continue
                
                print(f"Downloading {url}...")
                conn = http.client.HTTPSConnection("data.sba.gov")
                conn.request("GET", url)
                response = conn.getresponse()
                if response.status == 200:
                    decoded_content = response.read().decode('utf-8')
                    with open(filename, 'w', newline='') as f:
                        f.write(decoded_content)
                    print(f"CSV file saved as {filename}")
                    # saved_csvs.append(filename)
                else:
                    print(f"Failed to download {url}: {response.status} {response.reason}")
                conn.close()
            print("csvs downloaded")
            return "csvs downloaded"

        @task()
        def clean_data():
            """
            #### Clean task
            Cleans the data by dropping all the NaN fields in the data and combining it into one dataframe.
            """
            import os

            storage_folder = "./storage/ppp/"
            print("storage path exists", os.path.exists(storage_folder))
            if os.path.exists(storage_folder):
               
                csv_files = os.listdir(storage_folder)
            
                dfs = []
                for csv_file in csv_files:
                    df = pd.read_csv(csv_file)
                    print('printing df', df)
                # df_cleaned = df.dropna()
                # df_filtered = df_cleaned[(df_cleaned != 'Unanswered').all(axis=1)]
                    dfs.append(df)
                print('pringintg dfs', dfs[:2])
                concatDf = pd.concat(dfs, ignore_index=True)
                concatDf.to_csv(storage_folder + "cleaned_data.csv", index=False)
            print("csvs compiled and dropped NaN fields")
            return "csvs compiled and dropped NaN fields"

        @task()
        def analyze_data():
            """
            #### Analyze task
            Task calculates central measurement and spread from the dataframe.
            """
            import os
            import pandas as pd
            cleaned_data = "./storage/ppp/cleaned_data.csv"
            print("storage path exists", os.path.exists(cleaned_data))
            if os.path.exists(cleaned_data):
                df = pd.read_csv(cleaned_data)

                mean_loan_amount = df['CurrentApprovalAmount'].mean()
                median_loan_amount = df['CurrentApprovalAmount'].median()

                print('Mean Value:', mean_loan_amount)
                print('Median Value:', median_loan_amount)

                df['DateApproved'] = pd.to_datetime(df['DateApproved'])

                mean_date = df['DateApproved'].mean()
                median_date = df['DateApproved'].median()

                print('Mean Date Approved:', mean_date)
                print('Median Date Approved:', median_date)

                state_counts = df['BorrowerState'].value_counts()

                mean_state_count = state_counts.mean()
                median_state_count = state_counts.median()

                print('Mean State Count::', mean_state_count)
                print('Median State Count:', median_state_count)

                print(f"Amount of borrowers per state:\n {df['BorrowerState'].value_counts()}")
                print(f"Amount of businesses per type:\n{df['BusinessType'].value_counts()}")
                print(f"Amount of claimed non profit orgs:\n{df['NonProfit'].value_counts()}")
                print(f"Amount of businesses owned by gender:\n{df['Gender'].value_counts()}")
                print(f"Forgiveness Amount std, count, mean, min, and percentiles:\n{df['ForgivenessAmount'].describe()}")
                print(f"Current Approval Amount std, count, mean, min, and percentiles:\n{df['CurrentApprovalAmount'].describe()}")
                print(f"Undisbursed Amount std, count, mean, min, and percentiles:\n{df['UndisbursedAmount'].describe()}")
                print(f"Debt interest proceed std, count, mean, min, and percentiles:\n{df['DEBT_INTEREST_PROCEED'].describe()}")

        urls = [
                "https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/738e639c-1fbf-4e16-beb0-a223831011e8/download/public_150k_plus_230930.csv",
                "https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/a7fa66f4-fd2e-433c-8ef9-59780ef60ae5/download/public_up_to_150k_1_230930.csv",
                "https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/7d2308a8-0ac1-48a8-b21b-f9eb373ac417/download/public_up_to_150k_2_230930.csv",
                "https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/5158aae1-066d-4d01-a226-e44ecc9bdda7/download/public_up_to_150k_3_230930.csv",
                "https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/d888bab1-da5b-46f2-bed2-a052d48af246/download/public_up_to_150k_4_230930.csv",
                "https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/ee12d751-2bb4-4343-8330-32311ae4e7c7/download/public_up_to_150k_5_230930.csv",
                "https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/27b874d9-a059-4296-bb74-374294c48616/download/public_up_to_150k_6_230930.csv",
                "https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/434efae0-016a-48da-92dc-c6f113d827c1/download/public_up_to_150k_7_230930.csv",
                "https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/4fc8e993-c3b9-4eb2-b9bb-dfbde9b1fb6f/download/public_up_to_150k_8_230930.csv",
                "https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/7f9c6867-2b55-472e-a4f3-fd0f5f27f790/download/public_up_to_150k_9_230930.csv",
                "https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/a8f2c8b2-facb-4e97-ad5f-7c8736c8b4b6/download/public_up_to_150k_10_230930.csv",
                "https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/6f9787a3-afd6-45b2-b78e-ad0dc097c1c3/download/public_up_to_150k_11_230930.csv",
                "https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/b6528428-fbd9-4ca6-ae08-9e3416f8ee7f/download/public_up_to_150k_12_230930.csv"
            ]
        
        order_data = acquire_data(urls)
        cleaned_data = clean_data()
        
        order_data >> cleaned_data >> analyze_data()



    python_PPP_fraud_dag = PPP_fraud_detection()
