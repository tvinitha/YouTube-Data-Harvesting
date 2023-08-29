from googleapiclient.discovery import build
from streamlit_option_menu import option_menu
from pymongo import MongoClient
import pandas as pd
import mysql.connector
import re
import streamlit as st
import plotly.express as px

st.set_page_config(page_title="YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit", 
                   page_icon=None, layout="wide", initial_sidebar_state="auto", menu_items=None)


# Set page configuration
selected = option_menu(
        menu_title=None,
        options=["Home", "Extract Data","Migrate To SQL","Analysis"],
        orientation="horizontal",
        default_index=0, )
    
if selected == "Home":
    col1,col2 = st.columns(2,gap= 'medium')
    col1.markdown("## :blue[Domain] : Social Media")
    col1.markdown("## :blue[Technologies used] : Python,MongoDB, Youtube Data API, MySql, Streamlit")
    col1.markdown("## :clipboard: Overview:")
    col1.markdown("### :Retrieving YouTube channel data using the Google API, storing it in MongoDB as a data lake, migrating and transforming data into a SQL database, then querying and displaying data in a Streamlit app.")
    col2.markdown("#   ")
    col2.markdown("#   ")
    col2.markdown("#   ")

# Connect to MongoDB
import pymongo
client = pymongo.MongoClient('mongodb://localhost:27017')
db = client["yt_project"] 
collection = db["channel_details"]

# Connect to MySQL server
mydb = mysql.connector.connect(host="127.0.0.1",user="root",password="Vini@123",database="yt_project")
mycursor = mydb.cursor()

if selected == "Extract Data":
    st.write("### Enter YouTube Channel_ID below :")
        
    api_key ='AIzaSyBFl2MU1YGZn2e2iC9kgJ1Dk4aZ6Wwmmh0'
    youtube = build('youtube', 'v3', developerKey=api_key)

    def get_channel_data(channel_id):
        channel_detail = []
        
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id  
        )
        response = request.execute()

        for i in range(len(response['items'])):
                channel_data = dict(
                    channel_id=channel_id,  # Corrected key name here
                    channel_name=response['items'][i]['snippet']['title'],
                    playlist_id=response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    subscriber_count=int(response['items'][i]['statistics']['subscriberCount']),
                    video_count=int(response['items'][i]['statistics']['videoCount']),
                    view_count=int(response['items'][i]['statistics']['viewCount'])
                )
                channel_detail.append(channel_data)

        return channel_detail  

    #GET PLAYLIST DATA:

    def get_all_playlist_data(channel_id):        
        playlist_detail=[]
        next_page_token = None
        
        while True:
            response = youtube.playlists().list(
                part='snippet',
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token

            ).execute()  

            for i in range(len(response['items'])):
                playlist_data=dict(
                            channel_id = response['items'][i]['snippet']['channelId'],
                            channel_name = response['items'][i]['snippet']['channelTitle'],
                            playlist_id=response['items'][i]['id'],
                            playlist_name=response['items'][i]['snippet']['title'])
                playlist_detail.append(playlist_data)

            # Check if there are more pages of results
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                    break  # Break the loop if there are no more pages

        return playlist_detail

    # GET VIDEO IDS:

    def get_all_viodeo_ids(playlist_id):
        video_ids=[]
        next_page_token = None
        
        while True:
            request = youtube.playlistItems().list(
                part="snippet",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()

            for i in range(len(response['items'])):
                video_id=response['items'][i]['snippet']['resourceId']['videoId']
                video_ids.append(video_id)
                
            # Check if there are more pages of results
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                    break  # Break the loop if there are no more pages    
        return video_ids

    # GET VIDEO DETAILS:

    def get_all_video_details(video_ids):
        
        # convert duraations into sec
        def convert_duration_to_seconds(duration):
            regex = r'PT(\d+H)?(\d+M)?(\d+S)?'
            match = re.match(regex, duration)
            if not match:
                return 0

            hours, minutes, seconds = match.groups()
            hours = int(hours[:-1]) if hours else 0
            minutes = int(minutes[:-1]) if minutes else 0
            seconds = int(seconds[:-1]) if seconds else 0

            total_seconds = hours * 3600 + minutes * 60 + seconds
            return total_seconds 
        
        video_status = []
            
        for i in range(0, len(video_ids), 50):
            response = youtube.videos().list(
                part='snippet,contentDetails,statistics', 
                id=','.join(video_ids[i:i+50])
            ).execute()
            
            for i in range(len(response['items'])):                           
                video_details = dict(
                    channel_id = response['items'][i]['snippet']['channelId'],
                    channel_name = response['items'][i]['snippet']['channelTitle'],
                    video_id = response['items'][i]['id'],
                    Title = response['items'][i]['snippet']['title'],
                    Tags_str = ','.join(response['items'][i]['snippet'].get('tags', [])),
                    Thumbnails = response['items'][i]['snippet']['thumbnails']['default']['url'],
                    Description = response['items'][i]['snippet']['description'],
                    Publish_time = response['items'][i]['snippet']['publishedAt'],
                    duration = convert_duration_to_seconds(response['items'][i]['contentDetails']['duration']),
                    views = int(response['items'][i]['statistics']['viewCount']),
                    like = int(response['items'][i]['statistics'].get('likeCount', 0)),
                    comments = int(response['items'][i]['statistics'].get('commentCount', 0)),
                    Favorite_count = int(response['items'][i]['statistics'].get('favoriteCount', 0)),
                    Definition=response['items'][i]['contentDetails']['definition'],
                    Caption_status=response['items'][i]['contentDetails']['caption']
                )
                video_status.append(video_details)

        return video_status

    # GET COMMENT DETAILS:

    def get_all_comment_details(video_ids):
        comment_status = []
        
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=video_id
            )
            response = request.execute()
            
            total_comment_threads = response['pageInfo']['totalResults']
            
            if total_comment_threads == 0:
                continue  # Skip videos with disabled comments

            for i in range(len(response['items'])):
                comment_details = dict(
                    Comment_id = response['items'][i]['id'],
                    Video_id = response['items'][i]['snippet']['videoId'],
                    Comment_text = response['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'],
                    Comment_author = response['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    Comment_posted_date = response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'],
                    Like_count = int(response['items'][i]['snippet']['topLevelComment']['snippet'].get('likeCount', 0)),
                    Reply_count = int(response['items'][i]['snippet']['totalReplyCount'])
                )

                comment_status.append(comment_details)
        
        return comment_status

    def main_function(channel_id):

        channel_data = get_channel_data(channel_id)
        playlist_data =get_all_playlist_data(channel_id)    
        video_ids=get_all_viodeo_ids(channel_data[0]['playlist_id'])
        video_data=get_all_video_details(video_ids)
        comment_data=get_all_comment_details(video_ids)
        
        data = {
            'channel_data': channel_data,
            'playlist_data': playlist_data,
            'video_data': video_data,
            'comment_data': comment_data
        }
        return data
   
    channel_id = st.text_input("Hint: Go to channel's home page > Right click > View page source > Find channel_id")
    if channel_id:
        channel_details = main_function(channel_id)
        st.json(channel_details)  
              
    if st.button('Upload to MongoDB'):
        with st.spinner('Please Wait for it...'):
                collection.insert_one(channel_details)
                st.write("Document inserted successful.")

if selected == "Migrate To SQL":
    st.markdown("### Select a channel to begin Transformation to SQL")
    
    # Extract channel names from the documents in the collection
    channel_names = []
    # Loop through the documents in the collection and gather channel names
    for i in collection.find():
        channel_name = i["channel_data"][0]["channel_name"]
        channel_names.append(channel_name)

    # Add a default option to the list
    channel_names.insert(0, "Select a channel")

    # Create a dropdown menu for selecting a channel
    selected_channel_name = st.selectbox("Select a channel", channel_names)

    # Check if a valid channel is selected
    if selected_channel_name != "Select a channel":
        # Find the document for the selected channel in collection
        selected_document = next(
            (doc for doc in collection.find() if doc["channel_data"][0]["channel_name"] == selected_channel_name), None)
        
        if selected_document:
            # Process the selected channel data here
            channel_data = pd.DataFrame(selected_document['channel_data'])
            st.dataframe(channel_data)
            playlist_data = pd.DataFrame(selected_document['playlist_data'])
            st.dataframe(playlist_data)
            video_data = pd.DataFrame(selected_document['video_data'])            
            video_data['Publish_time'] = pd.to_datetime(video_data['Publish_time']).dt.strftime("%Y-%m-%d %H:%M:%S")
            st.dataframe(video_data)            
            comment_data = pd.DataFrame(selected_document['comment_data'])
            comment_data['Comment_posted_date'] = pd.to_datetime(comment_data['Comment_posted_date']).dt.strftime("%Y-%m-%d %H:%M:%S")
            st.dataframe(comment_data)
  
    if st.button('Migrate To SQL'):

        #channel_data
        sql_insert = "INSERT INTO channel_data (Channel_id, Channel_name, Playlist_id, Subscribers, Total_videos, Views) " \
                    "VALUES (%s, %s, %s, %s, %s, %s)"\
                    "ON DUPLICATE KEY UPDATE " \
                    "Channel_name = VALUES(Channel_name), Playlist_id = VALUES(Playlist_id), Subscribers = VALUES(Subscribers), " \
                    "Total_videos = VALUES(Total_videos), Views = VALUES(Views)"
        for _, row in channel_data.iterrows():
            values = tuple(row.values)    
            mycursor.execute(sql_insert, values)
        mydb.commit()

        #playlist _data
        sql_insert = "INSERT INTO playlist_data (Channel_id, Channel_name, Playlist_id, Playlist_name) " \
                    "VALUES (%s, %s, %s, %s)"\
                    "ON DUPLICATE KEY UPDATE " \
                    "Channel_name = VALUES(Channel_name), Playlist_id = VALUES(Playlist_id),Playlist_name=VALUES(Playlist_name)"
        for _, row in playlist_data.iterrows():
            values = tuple(row.values)    
            mycursor.execute(sql_insert, values)
        mydb.commit()

        #video_data
        sql_insert = "INSERT INTO video_data (Channel_id, Channel_name, Video_id, Title, Tags, Thumbnail, Description, Published_date, Duration, Views, Likes, Comments, Favorite_count, Definition, Caption_status) " \
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" \
                    "ON DUPLICATE KEY UPDATE " \
                    "Channel_name = VALUES(Channel_name), Title = VALUES(Title), " \
                    "Tags = VALUES(Tags), Thumbnail = VALUES(Thumbnail), Description = VALUES(Description), " \
                    "Published_date = VALUES(Published_date), Duration = VALUES(Duration), Views = VALUES(Views), " \
                    "Likes = VALUES(Likes), Comments = VALUES(Comments), Favorite_count = VALUES(Favorite_count), " \
                    "Definition = VALUES(Definition), Caption_status = VALUES(Caption_status)"
        for _, row in video_data.iterrows():
            values = tuple(row.values)    
            mycursor.execute(sql_insert, values)
        mydb.commit()

        #comment_data
        sql_insert = "INSERT INTO comment_data (Comment_id,Video_id,Comment_text,Comment_author,Comment_posted_date,Like_count,Reply_count ) " \
                    "VALUES (%s, %s, %s, %s, %s, %s,%s)"\
                    "ON DUPLICATE KEY UPDATE " \
                    "Comment_id = VALUES(Comment_id), Video_id = VALUES(Video_id),Comment_text=VALUES(Comment_text),"\
                    "Comment_author = VALUES(Comment_author), Comment_posted_date = VALUES(Comment_posted_date),Like_count=VALUES(Like_count),"\
                    "Reply_count = VALUES(Reply_count)"
        for _, row in comment_data.iterrows():
            values = tuple(row.values) 
            mycursor.execute(sql_insert, values)
        mydb.commit()
        
        st.write('Migrateed successfully')
        
if selected == "Analysis":   
    st.write("## :orange[Select any question to get Insights]")
    questions = st.selectbox('Questions',
    ['1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])

    if questions == '1. What are the names of all the videos and their corresponding channels?':
        Q1 ='''SELECT Title, channel_name from video_data 
               order by channel_name'''
        df = pd.read_sql_query(Q1, mydb)
        st.dataframe(df)
    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        Q2 ='''SELECT channel_name, Total_videos  
               FROM channel_data 
               order by Total_videos'''
        df = pd.read_sql_query(Q2, mydb)
        st.dataframe(df)
        st.write("### :green[Number of videos in each channel :]")
        #st.bar_chart(df,x= mycursor.column_names[0],y= mycursor.column_names[1])
        fig = px.bar(df,
             x=df.columns[0],  # Assuming the first column is 'channel_name'
             y=df.columns[1],  # Assuming the second column is 'Total_videos'
             orientation='v',
             color=df.columns[0]  # Assuming you want to color the bars by channel
            )
        st.plotly_chart(fig, use_container_width=True)

    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':         
        Q3 = '''select channel_name,Title as video_Titile, views  
                from video_data 
                order by views desc 
                limit 10'''
        df = pd.read_sql_query(Q3, mydb)
        st.dataframe(df)  
        st.write("### :green[Top 10 most viewed videos :]") 
        fig = px.bar(df,
             x=df.columns[1],  # Assuming 'channel_name' is the column name for channel names
             y=df.columns[2],  # Assuming 'views' is the column name for views
             orientation='v',
             color=df.columns[0]  # Color bars by channel
            )
        st.plotly_chart(fig, use_container_width=True)
               
    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        Q4 = '''SELECT Channel_name, Title, Comments AS Total_Comments
                FROM video_data'''
        df = pd.read_sql_query(Q4, mydb)
        st.dataframe(df)
        st.write("### :green[Number of Comments on Each channel :]")
        fig = px.bar(df,
             x=df.columns[0],  # Assuming 'Title' is the column name for video titles
             y=df.columns[2],  # Assuming 'Total_Comments' is the column name for total comments
            )
        st.plotly_chart(fig, use_container_width=True)
    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        Q5 = '''SELECT channel_name, Title, likes 
                FROM video_data 
                ORDER BY likes DESC LIMIT 10'''
        df = pd.read_sql_query(Q5, mydb)
        st.dataframe(df) 
        st.write("### :green[Number of views in each channel :]")
        fig = px.bar(df,
             x=df.columns[1],  # Assuming 'channel_name' is the column name for channel names
             y=df.columns[2],  # Assuming 'likes' is the column name for views
             orientation='v',
             color=df.columns[0]  # Color bars by channel
            )
        st.plotly_chart(fig, use_container_width=True)   
    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        Q6 = '''select channel_name,Title,likes 
                from video_data 
                order by likes desc'''
        df=pd.read_sql_query(Q6,mydb)
        st.dataframe(df)        
    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        Q7 = '''select channel_name,views 
                from channel_data 
                order by views desc'''
        df=pd.read_sql_query(Q7,mydb)
        st.dataframe(df)
        st.write("### :green[Channels vs Views :]")
        fig = px.bar(df,
                     x=df.columns[0],
                     y=df.columns[1],
                     orientation='v',
                     color=df.columns[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        Q8 = '''SELECT Channel_name,Title,published_date
                FROM video_data
                WHERE YEAR(Published_date) = 2022'''
        df=pd.read_sql_query(Q8,mydb)
        st.dataframe(df)
    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        Q9 = '''select channel_name,avg(duration) 
                from video_data 
                group by channel_name'''
        df = pd.read_sql_query(Q9, mydb)
        st.dataframe(df)
        st.write("### :green[average duration of all videos in each channel :]")
        fig = px.bar(df,
             x=df.columns[0],  # Assuming 'channel_name' is the column name for channel names
             y=df.columns[1],  # Assuming 'duration' is the column name for views
             orientation='v',
             color=df.columns[0]  # Color bars by channel
            )
        st.plotly_chart(fig, use_container_width=True)     
    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        Q10 = '''select channel_name,Title,comments 
                from video_data  
                order by comments desc limit 10'''
        df = pd.read_sql_query(Q10, mydb)
        st.dataframe(df)
        st.write("### :green[Videos with most comments :]")
        fig = px.bar(df,
             x=df.columns[1],  # Assuming 'channel_name' is the column name for channel names
             y=df.columns[2],  # Assuming 'comments' is the column name for views
             orientation='v',
             color=df.columns[0]  # Color bars by channel
            )
        st.plotly_chart(fig, use_container_width=True)                                  
        

