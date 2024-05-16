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
        def acquire_data1(urls, start, end):
            import os
            import http.client
            
            storage_folder = "/opt/airflow/storage/ppp"
            #if storage folder exists cd into it
            if os.path.exists(storage_folder):
                os.chdir(storage_folder)
           
            for i in range(start,end):
                filename = f"csv_{i}.csv" 
                if os.path.exists(filename):
                    print(f"CSV file {filename} already exists. Skipping download.")
                    
                    continue
                
                print(f"Downloading {urls[i]}...")
                conn = http.client.HTTPSConnection("data.sba.gov")
                conn.request("GET", urls[i])
                response = conn.getresponse()
                if response.status == 200:
                    decoded_content = response.read().decode('utf-8')
                    with open(filename, 'w', newline='') as f:
                        f.write(decoded_content)
                    print(f"CSV file saved as {filename}")
                    
                else:
                    print(f"Failed to download {urls[i]}: {response.status} {response.reason}")
                conn.close()
            print("csvs downloaded")
            return "csvs downloaded"
        
        @task.virtualenv(use_dill=True,
            system_site_packages=False,
            requirements=["pandas"])
        def acquire_data2(urls, start, end):
            import os
            import http.client
            
            storage_folder = "/opt/airflow/storage/ppp"
            #if storage folder exists cd into it
            if os.path.exists(storage_folder):
                os.chdir(storage_folder)
           
            for i in range(start,end):
                filename = f"csv_{i}.csv" 
                if os.path.exists(filename):
                    print(f"CSV file {filename} already exists. Skipping download.")
                    
                    continue
                
                print(f"Downloading {urls[i]}...")
                conn = http.client.HTTPSConnection("data.sba.gov")
                conn.request("GET", urls[i])
                response = conn.getresponse()
                if response.status == 200:
                    decoded_content = response.read().decode('utf-8')
                    with open(filename, 'w', newline='') as f:
                        f.write(decoded_content)
                    print(f"CSV file saved as {filename}")
                    
                else:
                    print(f"Failed to download {urls[i]}: {response.status} {response.reason}")
                conn.close()
            print("csvs downloaded")
            return "csvs downloaded"
        
        # @task.virtualenv(use_dill=True,
        #     system_site_packages=False,
        #     requirements=["pandas"])
        # def acquire_data3(urls, start, end):
        #     import os
        #     import http.client
            
        #     storage_folder = "/opt/airflow/storage/ppp"
        #     #if storage folder exists cd into it
        #     if os.path.exists(storage_folder):
        #         os.chdir(storage_folder)
           
        #     for i in range(start,end):
        #         filename = f"csv_{i}.csv" 
        #         if os.path.exists(filename):
        #             print(f"CSV file {filename} already exists. Skipping download.")
                    
        #             continue
                
        #         print(f"Downloading {urls[i]}...")
        #         conn = http.client.HTTPSConnection("data.sba.gov")
        #         conn.request("GET", urls[i])
        #         response = conn.getresponse()
        #         if response.status == 200:
        #             decoded_content = response.read().decode('utf-8')
        #             with open(filename, 'w', newline='') as f:
        #                 f.write(decoded_content)
        #             print(f"CSV file saved as {filename}")
                    
        #         else:
        #             print(f"Failed to download {urls[i]}: {response.status} {response.reason}")
        #         conn.close()
        #     print("csvs downloaded")
        #     return "csvs downloaded"
        
      

        # @task.virtualenv(use_dill=True,
        #     system_site_packages=False,
        #     requirements=["pandas"])
        # def acquire_data4(urls):
        #     import os
        #     import http.client
            
        #     storage_folder = "/opt/airflow/storage/ppp"
        #     #if storage folder exists cd into it
        #     if os.path.exists(storage_folder):
        #         os.chdir(storage_folder)
           
        #     for idx, url in enumerate(urls):
        #         filename = f"csv_{idx}.csv" 
        #         if os.path.exists(filename):
        #             print(f"CSV file {filename} already exists. Skipping download.")
                    
        #             continue
                
        #         print(f"Downloading {url}...")
        #         conn = http.client.HTTPSConnection("data.sba.gov")
        #         conn.request("GET", url)
        #         response = conn.getresponse()
        #         if response.status == 200:
        #             decoded_content = response.read().decode('utf-8')
        #             with open(filename, 'w', newline='') as f:
        #                 f.write(decoded_content)
        #             print(f"CSV file saved as {filename}")
                   
        #         else:
        #             print(f"Failed to download {url}: {response.status} {response.reason}")
        #         conn.close()
        #     print("csvs downloaded")
        #     return "csvs downloaded"

        @task()
        def place_holder_task1():
            return True

        @task()
        def clean_data1(start, end):
            """
            #### Clean task
            Cleans the data by dropping all the NaN fields in the data and compiles
            specified number of csvs.
            """
            import os

            storage_folder = "./storage/ppp/"
            
            if os.path.exists(storage_folder):
               
                dfs = []

                for i in range(start, end):
                    cleaned_csv_path = f"{storage_folder}cleaned_data_{start}.csv"
                    file_path = f"{storage_folder}csv_{i}.csv"
                    if os.path.exists(cleaned_csv_path):
                        print(f"CSV file {cleaned_csv_path} already exists. Skipping download.")

                        return

                    
                    print("reading file", file_path)
                    df = pd.read_csv(file_path)
                    # df_cleaned = df.fillna(0)
                    dfs.append(df)
                

                concatDf = pd.concat(dfs, ignore_index=True)
                
                concatDf.to_csv(cleaned_csv_path, index=False)
    
            return {f"compiled and cleaned csvs {start}-{end-1}": cleaned_csv_path}
        
        # @task()
        # def clean_data2(start, end):
        #     """
        #     #### Clean task
        #     Cleans the data by dropping all the NaN fields in the data and compiles
        #     specified number of csvs.
        #     """
        #     import os

        #     storage_folder = "./storage/ppp/"
            
        #     if os.path.exists(storage_folder):
               
        #         dfs = []

        #         for i in range(start, end):
        #             file_path = f"{storage_folder}csv_{i}.csv"
        #             cleaned_csv_path = f"{storage_folder}cleaned_data_{start}.csv"
        #             if os.path.exists(cleaned_csv_path):
        #                 print(f"CSV file {cleaned_csv_path} already exists. Skipping download.")

        #                 return
        #             print("reading file", file_path)
        #             df = pd.read_csv(file_path)
        #             df_cleaned = df.fillna(0)
        #             dfs.append(df_cleaned)
                

        #         concatDf = pd.concat(dfs, ignore_index=True)
                
        #         concatDf.to_csv(cleaned_csv_path, index=False)
    
        #     return {f"compiled and cleaned csvs {start}-{end-1}": cleaned_csv_path}
        
        # @task()
        # def clean_data3(start, end):
        #     """
        #     #### Clean task
        #     Cleans the data by dropping all the NaN fields in the data and compiles
        #     specified number of csvs.
        #     """
        #     import os

        #     storage_folder = "./storage/ppp/"
            
        #     if os.path.exists(storage_folder):
               
        #         dfs = []

        #         for i in range(start, end):
        #             file_path = f"{storage_folder}csv_{i}.csv"
        #             print("reading file", file_path)
        #             df = pd.read_csv(file_path)
        #             df_cleaned = df.fillna(0)
        #             dfs.append(df_cleaned)
                

        #         concatDf = pd.concat(dfs, ignore_index=True)
        #         cleaned_csv_path = f"{storage_folder}cleaned_data_{start}.csv"
        #         concatDf.to_csv(cleaned_csv_path, index=False)
    
        #     return {f"compiled and cleaned csvs {start}-{end-1}": cleaned_csv_path}
        
        # @task()
        # def clean_data4(start, end):
        #     """
        #     #### Clean task
        #     Cleans the data by dropping all the NaN fields in the data and compiles
        #     specified number of csvs.
        #     """
        #     import os

        #     storage_folder = "./storage/ppp/"
            
        #     if os.path.exists(storage_folder):
               
        #         dfs = []

        #         for i in range(start, end):
        #             file_path = f"{storage_folder}csv_{i}.csv"
        #             print("reading file", file_path)
        #             df = pd.read_csv(file_path)
        #             df_cleaned = df.fillna(0)
        #             dfs.append(df_cleaned)
                

        #         concatDf = pd.concat(dfs, ignore_index=True)
        #         cleaned_csv_path = f"{storage_folder}cleaned_data_{start}.csv"
        #         concatDf.to_csv(cleaned_csv_path, index=False)
    
        #     return {f"compiled and cleaned csvs {start}-{end-1}": cleaned_csv_path}
        # @task()
        # def clean_data5(start, end):
        #     """
        #     #### Clean task
        #     Cleans the data by dropping all the NaN fields in the data and compiles
        #     specified number of csvs.
        #     """
        #     import os

        #     storage_folder = "./storage/ppp/"
            
        #     if os.path.exists(storage_folder):
               
        #         dfs = []

        #         for i in range(start, end):
        #             file_path = f"{storage_folder}csv_{i}.csv"
        #             df = pd.read_csv(file_path)
        #             df_cleaned = df.fillna(0)
        #             dfs.append(df_cleaned)
                

        #         concatDf = pd.concat(dfs, ignore_index=True)
        #         cleaned_csv_path = f"{storage_folder}cleaned_data_{start}.csv"
        #         concatDf.to_csv(cleaned_csv_path, index=False)
    
        #     return {f"compiled and cleaned csvs {start}-{end-1}": cleaned_csv_path}
        
        # @task()
        # def clean_data6(start, end):
        #     """
        #     #### Clean task
        #     Cleans the data by dropping all the NaN fields in the data and compiles
        #     specified number of csvs.
        #     """
        #     import os

        #     storage_folder = "./storage/ppp/"
            
        #     if os.path.exists(storage_folder):
               
        #         dfs = []

        #         for i in range(start, end):
        #             file_path = f"{storage_folder}csv_{i}.csv"
        #             df = pd.read_csv(file_path)
        #             df_cleaned = df.fillna(0)
        #             dfs.append(df_cleaned)
                

        #         concatDf = pd.concat(dfs, ignore_index=True)
        #         cleaned_csv_path = f"{storage_folder}cleaned_data_{start}.csv"
        #         concatDf.to_csv(cleaned_csv_path, index=False)
    
        #     return {f"compiled and cleaned csvs {start}-{end-1}": cleaned_csv_path}
        
        @task()
        def place_holder_task2():
            return True

        @task()
        def filter_data1(num):
            """
            #### fiter task
            Filters the cleaned data by dropping all the unanswered fields in the data and
            saves as csv. Later in data visualization, will show cleaned data, and data
            with unanswered fields dropped
            """
            import os

            storage_folder = "./storage/ppp/"
            
            if os.path.exists(storage_folder):
                file_path = f"{storage_folder}cleaned_data_{num}.csv"
                filtered_csv_path = f"{storage_folder}filtered_data_{num}.csv"
                if os.path.exists(filtered_csv_path):
                    print(f"CSV file {filtered_csv_path} already exists. Skipping download.")

                    return
                
                df = pd.read_csv(file_path)
                df_filtered = df[(df != "Unanswered").all(axis=1)]
                
                df_filtered.to_csv(filtered_csv_path, index=False)
    
            return {f"filtered and cleaned csv": filtered_csv_path}

        # @task()
        # def filter_data2(num):
        #     """
        #     #### fiter task
        #     Filters the cleaned data by dropping all the unanswered fields in the data and
        #     saves as csv. Later in data visualization, will show cleaned data, and data
        #     with unanswered fields dropped
        #     """
        #     import os

        #     storage_folder = "./storage/ppp/"
            
        #     if os.path.exists(storage_folder):
        #         filtered_csv_path = f"{storage_folder}filtered_data_{num}.csv"
        #         file_path = f"{storage_folder}cleaned_data_{num}.csv"
        #         if os.path.exists(filtered_csv_path):
        #             print(f"CSV file {filtered_csv_path} already exists. Skipping download.")

        #             return
        #         df = pd.read_csv(file_path)
        #         df_filtered = df[(df != "Unanswered").all(axis=1)]
                
        #         df_filtered.to_csv(filtered_csv_path, index=False)
    
        #     return {f"filtered and cleaned csv": filtered_csv_path}
        
        # @task()
        # def filter_data3(num):
        #     """
        #     #### fiter task
        #     Filters the cleaned data by dropping all the unanswered fields in the data and
        #     saves as csv. Later in data visualization, will show cleaned data, and data
        #     with unanswered fields dropped
        #     """
        #     import os

        #     storage_folder = "./storage/ppp/"
            
        #     if os.path.exists(storage_folder):
            
        #         file_path = f"{storage_folder}cleaned_data_{num}.csv"
        #         df = pd.read_csv(file_path)
        #         df_filtered = df[(df != "Unanswered").all(axis=1)]
        #         filtered_csv_path = f"{storage_folder}filtered_data_{num}.csv"
        #         df_filtered.to_csv(filtered_csv_path, index=False)
    
        #     return {f"filtered and cleaned csv": filtered_csv_path}
        
        # @task()
        # def filter_data4(num):
        #     """
        #     #### fiter task
        #     Filters the cleaned data by dropping all the unanswered fields in the data and
        #     saves as csv. Later in data visualization, will show cleaned data, and data
        #     with unanswered fields dropped
        #     """
        #     import os

        #     storage_folder = "./storage/ppp/"
            
        #     if os.path.exists(storage_folder):
            
        #         file_path = f"{storage_folder}cleaned_data_{num}.csv"
        #         df = pd.read_csv(file_path)
        #         df_filtered = df[(df != "Unanswered").all(axis=1)]
        #         filtered_csv_path = f"{storage_folder}filtered_data_{num}.csv"
        #         df_filtered.to_csv(filtered_csv_path, index=False)
    
        #     return {f"filtered and cleaned csv": filtered_csv_path}
        
        # @task()
        # def filter_data5(num):
        #     """
        #     #### fiter task
        #     Filters the cleaned data by dropping all the unanswered fields in the data and
        #     saves as csv. Later in data visualization, will show cleaned data, and data
        #     with unanswered fields dropped
        #     """
        #     import os

        #     storage_folder = "./storage/ppp/"
            
        #     if os.path.exists(storage_folder):
            
        #         file_path = f"{storage_folder}cleaned_data_{num}.csv"
        #         df = pd.read_csv(file_path)
        #         df_filtered = df[(df != "Unanswered").all(axis=1)]
        #         filtered_csv_path = f"{storage_folder}filtered_data_{num}.csv"
        #         df_filtered.to_csv(filtered_csv_path, index=False)
    
        #     return {f"filtered and cleaned csv": filtered_csv_path}
        
        # @task()
        # def filter_data6(num):
        #     """
        #     #### fiter task
        #     Filters the cleaned data by dropping all the unanswered fields in the data and
        #     saves as csv. Later in data visualization, will show cleaned data, and data
        #     with unanswered fields dropped
        #     """
        #     import os

        #     storage_folder = "./storage/ppp/"
            
        #     if os.path.exists(storage_folder):
            
        #         file_path = f"{storage_folder}cleaned_data_{num}.csv"
        #         df = pd.read_csv(file_path)
        #         df_filtered = df[(df != "Unanswered").all(axis=1)]
        #         filtered_csv_path = f"{storage_folder}filtered_data_{num}.csv"
        #         df_filtered.to_csv(filtered_csv_path, index=False)
    
        #     return {f"filtered and cleaned csv": filtered_csv_path}

        @task
        def compile_clean_data():
            """
            Compiles all cleaned csv into one csv
            """
            import os

            storage_folder = "./storage/ppp/"

            if os.path.exists(storage_folder):
                compiled_cleaned_csv_path = f"{storage_folder}compiled_cleaned_data.csv"
                if os.path.exists(compiled_cleaned_csv_path):
                    print(f"CSV file {compiled_cleaned_csv_path} already exists. Skipping download.")

                    return
                files = os.listdir(storage_folder)
                dfs = []

                for file in files:
                    #only compile cleaned csv files
                    if "cleaned_data" in file:
                        file_path = f"{storage_folder}{file}"
                        df = pd.read_csv(file_path)
                        dfs.append(df)

            concatDf = pd.concat(dfs, ignore_index=True)

            
            concatDf.to_csv(compiled_cleaned_csv_path, index=False)
    
            return {f"compiled all clean csvs": compiled_cleaned_csv_path}

       
        @task
        def compile_filtered_data():
            """
            Compiles all filtered csvs into one csv
            """
            import os

            storage_folder = "./storage/ppp/"

            if os.path.exists(storage_folder):
                compiled_filtered_csv_path = f"{storage_folder}compiled_filtered_data.csv"
                if os.path.exists(compiled_filtered_csv_path):
                    print(f"CSV file {compiled_filtered_csv_path} already exists. Skipping download.")

                    return
                files = os.listdir(storage_folder)
                dfs = []

                for file in files:
                    #only compile filtered csv files
                    if "filtered_data" in file:
                        file_path = f"{storage_folder}{file}"
                        df = pd.read_csv(file_path)
                        dfs.append(df)

            concatDf = pd.concat(dfs, ignore_index=True)

            
            concatDf.to_csv(compiled_filtered_csv_path, index=False)
    
            return {f"compiled all filtered csvs": compiled_filtered_csv_path}


        @task()
        def analyze_data():
            """
            #### Analyze task
            Calculates and prints central measurement and spread from the dataframe.
            Also saves CSV of calculations
            """
            import os
            import pandas as pd
            cleaned_data = "./storage/ppp/compiled_cleaned_data.csv"
            print("storage path exists", os.path.exists(cleaned_data))
            if os.path.exists(cleaned_data):
                df = pd.read_csv(cleaned_data)

                mean_loan_amount = df['CurrentApprovalAmount'].mean()
                median_loan_amount = df['CurrentApprovalAmount'].median()

                print('Mean current loan approval amount:', mean_loan_amount)
                print('Median current loan approval amount:', median_loan_amount)

                df['DateApproved'] = pd.to_datetime(df['DateApproved'])

                mean_date = df['DateApproved'].mean()
                median_date = df['DateApproved'].median()

                print('Mean Date Approved:', mean_date)
                print('Median Date Approved:', median_date)

                state_counts = df['BorrowerState'].value_counts()

                mean_state_count = state_counts.mean()
                median_state_count = state_counts.median()

                print('Mean number of borrowers of all states:', mean_state_count)
                print('Median number of borrowers of all states:', median_state_count)

                borrowers_per_state = df['BorrowerState'].value_counts()
                businesses_per_type = df['BusinessType'].value_counts()
                non_profit_count = df['NonProfit'].value_counts()
                owned_by_gender = df['Gender'].value_counts()
                forgivness_stats = df['ForgivenessAmount'].describe()
                current_approval_stats = df['CurrentApprovalAmount'].describe()
                undisbursed_stats = df['UndisbursedAmount'].describe()
                debt_interest_stats = df['DEBT_INTEREST_PROCEED'].describe()

                print(f"Amount of borrowers per state:\n {borrowers_per_state}")
                print(f"Amount of businesses per type:\n{businesses_per_type}")
                print(f"Amount of claimed non profit orgs:\n{non_profit_count}")
                print(f"Amount of businesses owned by gender:\n{owned_by_gender}")
                print(f"Forgiveness Amount std, count, mean, min, and percentiles:\n{forgivness_stats}")
                print(f"Current Approval Amount std, count, mean, min, and percentiles:\n{current_approval_stats}")
                print(f"Undisbursed Amount std, count, mean, min, and percentiles:\n{undisbursed_stats}")
                print(f"Debt interest proceed std, count, mean, min, and percentiles:\n{debt_interest_stats}")
                
                analysis_results = pd.DataFrame({
                    'Mean Loan Amount': [mean_loan_amount],
                    'Median Loan Amount': [median_loan_amount],
                    'Mean Date Approved': [mean_date],
                    'Median Date Approved': [median_date],
                    'Mean State Count': [state_counts.mean()],
                    'Median State Count': [state_counts.median()],
                    'Borrowers Per State': [borrowers_per_state],
                    'Businesses Per Type': [businesses_per_type],
                    'Non-profit Count': [non_profit_count],
                    'Businesses by Gender': [owned_by_gender],
                    'Forgiveness Statistics': [forgivness_stats],
                    'Current Approval Statistics': [current_approval_stats],
                    'Undisbursed Statistics': [undisbursed_stats],
                    'Debt Interest Statistics': [debt_interest_stats]
                })

                analysis_results_path = './storage/ppp/analysis_results.csv'
                analysis_results.to_csv(analysis_results_path, index=False)

                return {'status': 'success', 'data_path': analysis_results_path}

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
        
        
        
        
        [acquire_data1(urls,0,13)] >> place_holder_task1() >> [clean_data1(0,13)] >> place_holder_task2() >> [filter_data1(13)]  >> analyze_data()



    python_PPP_fraud_dag = PPP_fraud_detection()
