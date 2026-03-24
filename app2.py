import streamlit as st 
import pandas as pd 
import matplotlib.pyplot as plt 
import plotly.express as px 

st.set_page_config(page_title='Streamlit Visualization',layout='wide')

st.header('**Customer Orders Visualization**')

col1,col2,col3,col4,col5 = st.columns(5)

data = pd.read_csv(r"C:\Users\HP\Desktop\martha's engineering project\customer's and order's dataset.csv")

col1.metric('Total Customers',data['customer_id'].nunique())

col2.metric('Total Revenue',f"{data['order_amount'].sum():,.0f}")

col3.metric('Total Orders',f"{data['order_id'].count():,}")

col4.metric('Completed orders',f"{len(data[data['order_status']== 'completed']):,}")

col5.metric('Average Order Amount',f"{data['order_amount'].mean():,.0f}")