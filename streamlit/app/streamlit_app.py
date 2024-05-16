# -*- coding: utf-8 -*-
# Copyright 2018-2022 Streamlit Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""An example of showing geographic data."""

import os

import altair as alt
import numpy as np
import pandas as pd
import pydeck as pdk
import streamlit as st

# SETTING PAGE CONFIG TO WIDE MODE AND ADDING A TITLE AND FAVICON
st.set_page_config(layout="wide", page_title="PPP Loans Analysis", page_icon=":money_with_wings:")

from pyecharts import options as opts
from pyecharts.charts import Pie
from streamlit_echarts import st_pyecharts

@st.cache_data
def show_filtered_over_150k():
    df = pd.read_csv('../../airflow/storage/filtered_data_0.csv')
    # Convert 'DateApproved' column to datetime
    df['DateApproved'] = pd.to_datetime(df['DateApproved'])
    df['ForgivenessDate'] = pd.to_datetime(df['ForgivenessDate'])

    # Title of the app
    st.title('Number of PPP Approved per Gender over Date Approved')

    # Line chart for Gender over DateApproved
    st.line_chart(df.groupby('DateApproved')['Gender'].value_counts().unstack().fillna(0))

    st.title('Number of PPP Borrowers per State over Date Approved')


    st.line_chart(df.groupby('DateApproved')['BorrowerState'].value_counts().unstack())

    st.title('Number of PPP Approved per Ethnicity over Date Approved')

    st.line_chart(df.groupby('DateApproved')['Ethnicity'].value_counts().unstack())

    st.title('Number of PPP Approved per Business Age over Date Approved')


    st.line_chart(df.groupby('DateApproved')['BusinessAgeDescription'].value_counts().unstack())

    st.title('Average Current Approval Amount over Date Approved')
    
    # Group by DateApproved and calculate average currentApprovalAmount
    avg_approval_amount = df.groupby('DateApproved')['CurrentApprovalAmount'].mean().reset_index()

    st.line_chart(avg_approval_amount.set_index('DateApproved'))

    # Group by 'DateApproved' and count the number of loans (rows) for each group
    loan_counts = df.groupby('DateApproved').size()



    st.title('Average Number of Loans Approved per Date Approved')

    # Line chart for average number of loans per DateApproved
    st.line_chart(loan_counts)

    st.title('Average Forgiveness Amount over ForgivenessDate')
    # Group by DateApproved and calculate average currentApprovalAmount
    avg_forgiveness_amount = df.groupby('ForgivenessDate')['ForgivenessAmount'].mean().reset_index()

    st.line_chart(avg_forgiveness_amount.set_index('ForgivenessDate'))

    st.title("Number of businesses per type")
    business_type_counts = df['BusinessType'].value_counts()
    st.bar_chart(business_type_counts)


    st.title("PPP borrowers businesses in rural vs urban area")
    rural_urban_type_counts = df['RuralUrbanIndicator'].value_counts()
    st.bar_chart(rural_urban_type_counts)

    # Convert business_type_counts to list of tuples for Pyecharts
    rural_urban_count = [(key, value) for key, value in rural_urban_type_counts.items()]

    pie_chart2 = (
        Pie()
        .add("", rural_urban_count)
        .set_global_opts(title_opts=opts.TitleOpts(title="PPP borrowers businesses in rural vs urban area"))
        .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {d}%"))
    )

    st_pyecharts(pie_chart2)


@st.cache_data
def show_filtered_upto_150k():
    df = pd.read_csv('../../airflow/storage/filtered_data_1.csv')
    # Convert 'DateApproved' column to datetime
    df['DateApproved'] = pd.to_datetime(df['DateApproved'])
    df['ForgivenessDate'] = pd.to_datetime(df['ForgivenessDate'])

    # Title of the app
    st.title('Number of PPP Approved per Gender over Date Approved')

    # Line chart for Gender over DateApproved
    st.line_chart(df.groupby('DateApproved')['Gender'].value_counts().unstack().fillna(0))

    st.title('Number of PPP Borrowers per State over Date Approved')


    st.line_chart(df.groupby('DateApproved')['BorrowerState'].value_counts().unstack())

    st.title('Number of PPP Approved per Ethnicity over Date Approved')

    st.line_chart(df.groupby('DateApproved')['Ethnicity'].value_counts().unstack())

    st.title('Number of PPP Approved per Business Age over Date Approved')


    st.line_chart(df.groupby('DateApproved')['BusinessAgeDescription'].value_counts().unstack())

    st.title('Average Current Approval Amount over Date Approved')
    
    # Group by DateApproved and calculate average currentApprovalAmount
    avg_approval_amount = df.groupby('DateApproved')['CurrentApprovalAmount'].mean().reset_index()

    st.line_chart(avg_approval_amount.set_index('DateApproved'))

    # Group by 'DateApproved' and count the number of loans (rows) for each group
    loan_counts = df.groupby('DateApproved').size()

    # Calculate the average number of loans per 'DateApproved'
    average_loans_per_date = loan_counts.mean()

    st.title('Average Number of Loans Approved per Date Approved')

    # Line chart for average number of loans per DateApproved
    st.line_chart(loan_counts)

    st.title('Average Forgiveness Amount over ForgivenessDate')
    # Group by DateApproved and calculate average currentApprovalAmount
    avg_forgiveness_amount = df.groupby('ForgivenessDate')['ForgivenessAmount'].mean().reset_index()

    st.line_chart(avg_forgiveness_amount.set_index('ForgivenessDate'))

    st.title("Number of businesses per type")
    business_type_counts = df['BusinessType'].value_counts()
    st.bar_chart(business_type_counts)


    st.title("PPP borrowers businesses in rural vs urban area")
    rural_urban_type_counts = df['RuralUrbanIndicator'].value_counts()
    st.bar_chart(rural_urban_type_counts)

    # Convert business_type_counts to list of tuples for Pyecharts
    rural_urban_count = [(key, value) for key, value in rural_urban_type_counts.items()]

    pie_chart2 = (
        Pie()
        .add("", rural_urban_count)
        .set_global_opts(title_opts=opts.TitleOpts(title="PPP borrowers businesses in rural vs urban area"))
        .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {d}%"))
    )

    st_pyecharts(pie_chart2)



dataset = st.selectbox(
    "Select which dataset you want to visualize",
    ("filtered data of loans over $150k", "filtered of loans up to $150k"),
    index=None,
    placeholder="Choose dataset..."
)

if dataset == "filtered data of loans over $150k":
    show_filtered_over_150k()
elif dataset == "filtered of loans up to $150k":
    show_filtered_upto_150k()