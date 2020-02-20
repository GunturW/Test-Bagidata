#!/usr/bin/env python
# coding: utf-8



import json
import facebook
import csv
import pandas as pd
import sqlalchemy
from pandas.io.json import json_normalize

def get_json_profile(profile):
    #saving profile data into profile.json
    with open('profile.json','a') as file:
        file.write(json.dumps(profile, indent=4))
    
    
def get_json_posts(post_data, reaction):
    #saving posts data into post_data.json
    with open('post_data.json','a') as file:
        file.write(json.dumps(post_data, indent=4))
    
    #saving reaction data into reaction.json
    with open('reaction.json','a') as file:
        file.write(json.dumps(reaction, indent=4))

def get_csv_profile(profile):
    #flattening json data to pandas Dataframe
    profile_df = json_normalize(profile)
    
    #converting Dataframe to CSV
    profiles_csv = profile_df.to_csv()
    
    #saving profile data into profile.csv
    with open("profile.csv",'w') as file:
        file.write(profiles_csv)
        
def get_csv_posts(post_data, reaction):
    #flattening json data to pandas Dataframe
    post_data_df = json_normalize(post_data,record_path=['posts','data'])
    
    #converting Dataframe to CSV
    post_data_csv = post_data_df.to_csv()
    
    #saving posts data into post_data.csv
    with open('post_data.csv','w') as file:
        file.write(post_data_csv)
    
    #flattening json data to pandas Dataframe
    reaction_df = json_normalize(reaction)
    
    #converting Dataframe to CSV
    reaction_csv = reaction_df.to_csv()
    
    #saving reaction data into reaction.csv
    with open('reaction.csv','w') as file:
        file.write(reaction_csv)

def get_sql_profile(profile):
    #load data from profile.csv
    csv_data = pd.read_csv("profile.csv")
    
    #create engine to connecting on facebook database
    engine = sqlalchemy.create_engine('mysql+pymysql://root:1234@localhost:3306/facebook')
    
    #converting Dataframe to sql with table name 'profile'
    profile_sql = csv_data.to_sql(name='profile',con=engine)
    
    #create a cursor for executing on database
    c = engine.connect()
    con = c.connection
    cursor = con.cursor()
    
    #create query for grab all data from profile table
    query = "SELECT * FROM profile;"
    cursor.execute(query)
    
    #saving profile data into profile.sql
    with open('profile.sql','w',) as file:
        for row in cursor.fetchall():
            file.write(str(row))
            file.write(",")
            file.write("\n")
    cursor.close()
    
def get_sql_posts(post_data, reaction):
    #load data from post_data.csv
    csv_data_posts = pd.read_csv("post_data.csv")
    
    #create engine to connecting on facebook database
    engine = sqlalchemy.create_engine('mysql+pymysql://root:1234@localhost:3306/facebook')
    
    #converting Dataframe to sql with table name 'posts'
    profile_sql = csv_data_posts.to_sql(name='posts',con=engine)
    
    #create a cursor for executing on database
    c = engine.connect()
    con = c.connection
    cursor = con.cursor()
    
    #create query for grab all data from posts table
    query = "SELECT * FROM posts;"
    cursor.execute(query)
    
    #saving profile data into posts.sql
    with open('posts.sql','w',) as file:
        for row in cursor.fetchall():
            file.write(str(row))
            file.write(",")
            file.write("\n")
    cursor.close()
    
    #load data from reaction.csv
    csv_data_reaction = pd.read_csv("reaction.csv")
    
    #create engine to connecting on facebook database
    engine = sqlalchemy.create_engine('mysql+pymysql://root:1234@localhost:3306/facebook')
    
    #converting Dataframe to sql with table name 'reaction'
    profile_sql = csv_data_posts.to_sql(name='reaction',con=engine)
    
    #create a cursor for executing on database
    c = engine.connect()
    con = c.connection
    cursor = con.cursor()
    
    #create query for grab all data from reaction table
    query = "SELECT * FROM reaction;"
    cursor.execute(query)
    
    #saving profile data into reaction.sql
    with open('reaction.sql','w',) as file:
        for row in cursor.fetchall():
            file.write(str(row))
            file.write(",")
            file.write("\n") 
    cursor.close()
                                 
def main():
    #create token from Facebook Graph API ( token wil be expired once an hour)
    token = {'EAAIqn1hLfH4BAIed8BDxAx9uDhj3zojInwMaxbQlyqpekWbWGY1E4CzO7dxhwNgxB9KFZBAoJi6UPjo84CLJnyt3UXiEesZCfFBY1u47sGZCKSSdKUzYNOfhjMihMOaQTlp4Ix5vZCqhjpDgO6Mum9RiAZB2twTiNdJRgbd3EpD5lJB55KStcQN3XesF0XzuZCUXujetflZB06SEL7LUDK0eIS44GT5I6vQqklzrTHiRwZDZD'}
    graph = facebook.GraphAPI(token)
    
    '''
        We must create fields what we need. On Graph API User (https://developers.facebook.com/docs/graph-api/reference/user/)
        relationship_status has returns no data as of April 4, 2018
    '''
    fields_profile = {'name','id','picture','cover','about','relationship_status','birthday','friends'}
    fields_profile = ','.join(fields_profile)
    profile = graph.get_object('me', fields=fields_profile)
    
    #create fields in posts : name, created_time, link
    fields_posts = {'posts.fields(name, created_time, link)',}
    fields_posts = ','.join(fields_posts)
    fields_posts = graph.get_object('me', fields=fields_posts)
    
    #create fields to call reaction from data
    reaction = fields_posts['posts']['data'][0]
    reaction_data = graph.get_object(id=reaction['id'], fields="""message, comments, reactions.summary(total_count)""")
    
    #called the function
    get_json_profile(profile)
    get_json_posts(fields_posts, reaction_data)
    
    get_csv_profile(profile)
    get_csv_posts(fields_posts, reaction_data)
    
    get_sql_profile(profile)
    get_sql_posts(fields_posts, reaction_data)
    
if __name__ == '__main__':
    main()







