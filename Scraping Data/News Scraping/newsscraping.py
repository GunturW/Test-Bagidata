#!/usr/bin/env python
# coding: utf-8

# In[4]:


import os
import requests
import pandas as pd
import json
import csv
import sqlalchemy

def get_json(url, parameters):
    #create variable response from url and parameters
    response = requests.get(url, params=parameters)
    
    #change data to json
    response_json = response.json()
    
    #get the title from news and make to Dataframe
    title_news = []
    for news in response_json['articles']:
        title_news.append(news['title'])
    
    #create pandas Dataframe
    title_df = pd.DataFrame(title_news, columns=['Title'] )
    title_df = title_df.to_json()

    #saving data into title_news.json
    with open('title_news.json','a') as file:
        file.write(json.dumps(title_df))

def get_csv(url, parameters):
    #create variable response from url and parameters
    response = requests.get(url, params=parameters)
    
    #change data to json
    response_json = response.json()
    
    #get the title from news and make to Dataframe
    title_news = []
    for news in response_json['articles']:
        title_news.append(news['title'])
        
    #create pandas Dataframe
    title_df = pd.DataFrame(title_news, columns=['Title'])
    title_csv = title_df.to_csv()
    
    #saving data into title_news.csv
    with open("title_news.csv","w") as file:
        file.write(title_csv)
        
def get_sql(url, parameters):
    #create variable response from url and parameters
    response = requests.get(url, params=parameters)
    
    #change data to json
    response_json = response.json()
    
    #get the title from news and make to Dataframe
    title_news = []
    for news in response_json['articles']:
        title_news.append(news['title'])
    
    #create pandas Dataframe
    title_df = pd.DataFrame(title_news, columns=['Title'])
    
    #create engine to connecting on news database
    engine = sqlalchemy.create_engine('mysql+pymysql://root:1234@localhost:3306/news')
    
    #converting Dataframe to sql with table name 'TitleNews'
    title_sql = title_df.to_sql(name='title_news',con=engine)
    
    #create a cursor for executing on database
    c = engine.connect()
    con = c.connection
    cursor = con.cursor()
    
    #create query for grab all data from profile table
    query = "SELECT * FROM title_news;"
    cursor.execute(query)
    
    #saving data into title_news.sql
    with open('title_news.sql','w',) as file:
        for row in cursor.fetchall():
            file.write(str(row))
            file.write(",")
            file.write("\n")

def main():
    #create URL from News API
    url = "https://newsapi.org/v2/everything?"
    
    #create api_key from your account
    api_key = "84009f8f8b0c460fa8392c330720a3a0"
    
    '''create parameters (cause we want to find about 'finance', we can put finance)
        Data from January 1, 2020
    '''
    parameters = {
        'q' : 'finance',
        'pagesize': 100,
        'apikey': api_key,
        'from': 2020-1-1
    }
    
    #called the function
    get_json(url, parameters)
    get_csv(url, parameters)
    get_sql(url, parameters)
    
if __name__ == '__main__':
    main()
    


# In[ ]:





# In[ ]:




