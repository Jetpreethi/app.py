import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import pymongo
import mysql.connector as sql
from googleapiclient.discovery import build
from datetime import datetime
import logging    # using logging it shows exact error

# setting the page configurations

st.set_page_config(page_title="Youtube Data Harvesting and warehousing| By Preethi ",
                   #page_icon=icon,
                   layout="wide",
                   initial_sidebar_state="expanded",
                   menu_items={'About':""" #This app is created by Preethi!*"""})


# CREATING OPTION MENU
with st.sidebar:
    selected = option_menu(None, ["Home","Extract-migrate & Transfer","View"], 
                           icons=["house-door-fill","tools","eye"],
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "20px", "text-align": "centre", "margin": "0px", 
                                                "--hover-color": "#27A40A"},
                                   "icon": {"font-size": "20px"},
                                   "container" : {"max-width": "2000px"},
                                   "nav-link-selected": {"background-color": "#27A40A"}})

# Bridging a connection with MongoDB Atlas and Creating a new database(youtube_data)
client = pymongo.MongoClient("mongodb+srv://preethi:Ammu1234@cluster1.0ojjx3y.mongodb.net/?retryWrites=true&w=majority")
db = client.youtube_data

# Create a connection to the MySQL database
mydb = sql.connect(
    host="localhost",
    user="root",
    password="Preethi@123",
    database="youtube"
)

# Create a cursor to execute SQL commands
mycursor = mydb.cursor(buffered=True)



# BUILDING CONNECTION WITH YOUTUBE API
api_key ="AIzaSyBL4FxybjGrvTv9e3vD2-ieBfNEa17lQQg"
youtube = build('youtube','v3',developerKey=api_key)  


# FUNCTION TO GET CHANNEL DETAILS
def get_channel_details(channel_id):
    ch_data = []
    response = youtube.channels().list(part='snippet,contentDetails,statistics', id=channel_id).execute()

    for i in range(len(response['items'])):
        data = dict(Channel_id=channel_id[i],
                    Channel_name=response['items'][i]['snippet']['title'],
                    Upload_id=response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    Subscribers=response['items'][i]['statistics']['subscriberCount'],
                    Views=response['items'][i]['statistics']['viewCount'],
                    Total_videos=response['items'][i]['statistics']['videoCount'],
                    Description=response['items'][i]['snippet']['description'],
                    Country=response['items'][i]['snippet'].get('country')
                    )
        ch_data.append(data)
    return ch_data

# FUNCTION TO GET VIDEO IDS
def get_channel_videos(channel_id):
    video_ids = []

    # get Uploads playlist id
    res = youtube.channels().list(id=channel_id, part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        res = youtube.playlistItems().list(playlistId=playlist_id, part='snippet', maxResults=50, pageToken=next_page_token).execute()

        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')

        if next_page_token is None:
            break
        logging.info(f"Retrieved {len(video_ids)} video IDs. Next Page Token: {next_page_token}")

    return video_ids


# FUNCTION TO GET VIDEO DETAILS
def get_video_details(video_ids):
    video_stats = []

    for i in range(0, len(video_ids), 50):
        response = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i+50])
        ).execute()

        for video in response['items']:
            # Convert ISO 8601 date to MySQL datetime format
            published_date_iso = video['snippet']['publishedAt']
            published_date_mysql = datetime.strptime(published_date_iso, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')

            video_details = {
                'Channel_name': video['snippet']['channelTitle'],
                'Channel_id': video['snippet']['channelId'],
                'Video_id': video['id'],
                'Title': video['snippet']['title'],
                'Thumbnail': video['snippet']['thumbnails']['default']['url'],
                'Description': video['snippet']['description'],
                'Published_date': published_date_mysql,
                'Duration': video['contentDetails']['duration'],
                'Views': video['statistics']['viewCount'],
                'Likes': video['statistics'].get('likeCount'),
                'Comments': video['statistics'].get('commentCount'),
                'Favorite_count': video['statistics']['favoriteCount'],
                'Definition': video['contentDetails']['definition'],
                'Caption_status': video['contentDetails']['caption']
            }
            video_stats.append(video_details)
            
        logging.info(f"Retrieved details for {len(video_stats)} videos. Remaining video IDs: {len(video_ids) - (i + 50)}")

    return video_stats




# FUNCTION TO GET COMMENT DETAILS
def get_comments_details(v_id):
    comment_data = []
    try:
        next_page_token = None
        comment_count = 0
        while comment_count < 100:  # Limit to 100 comments
            response = youtube.commentThreads().list(part="snippet,replies",
                                                     videoId=v_id,
                                                     maxResults=50,
                                                     pageToken=next_page_token).execute()
            for cmt in response['items']:
                data = dict(Comment_id=cmt['id'],
                            Video_id=cmt['snippet']['videoId'],
                            Comment_text=cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author=cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date=cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Like_count=cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count=cmt['snippet']['totalReplyCount']
                            )
                comment_data.append(data)
                
            next_page_token = None
            if next_page_token is None:
                break
    except:
       pass
    return comment_data





# Example input datetime string
def format_datetime_for_mysql(input_datetime_str):
    parsed_datetime = datetime.strptime(input_datetime_str, '%Y-%m-%dT%H:%M:%SZ')
    formatted_datetime_str = parsed_datetime.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_datetime_str

# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
def channel_names():   
    ch_name = []
    for i in db.channel_details.find():
        ch_name.append(i['Channel_name'])
    return ch_name

# FUNCTION FOR GETTING CHANNEL LIST

def channel_list():
    channel_list = []
    for i in db.channel_details.find():
        channel_list.append(i['Channel_name'])

    if channel_list != []:
        return channel_list
    else:
        channel_list = ["NO COLLECTION TO DISPLAY PLEASE EXTRACT !!!"]
        return channel_list
    
    # FUNCTION TO COLLECT ALL THE COMMENTS FOR RESPECTIVE CHANNEL AND THEIR VIDEO_IDS


def get_comments(v_ids):
    com_d = []
    for i in v_ids:
        com_d = com_d + get_comments_details(v_id=i)
    return com_d


# CREAING A TABLE SCHEMA FOR MYSQL TABLES
def create_mysql_tables():
    mycursor.execute("""CREATE TABLE IF NOT EXISTS channel_details (
    Channel_id VARCHAR(40) PRIMARY KEY,
    Channel_name VARCHAR(40),
    Upload_id VARCHAR(40),
    Subscribers BIGINT,
    Views BIGINT,
    Total_videoes INT,
    Description VARCHAR(100),
    Country VARCHAR(10))""")

    mycursor.execute("""CREATE TABLE IF NOT EXISTS video_details (
    Channel_name VARCHAR(200),
    Channel_id VARCHAR(40),
    Video_id VARCHAR(20) PRIMARY KEY,
    Title VARCHAR(250),
    Thumbnail VARCHAR(100),
    Description LONGTEXT,                
    Published_date VARCHAR(30),
    Duration VARCHAR(30),
    Views BIGINT,
    Likes BIGINT,
    Comments BIGINT,
    Favorite_count BIGINT,
    Definition VARCHAR(200),
    Caption_status VARCHAR(50)                                                    
    )""")

    mycursor.execute("""CREATE TABLE IF NOT EXISTS comments (
    Comment_id VARCHAR(30) PRIMARY KEY,
    Video_id VARCHAR(30),
    Comment_text MEDIUMTEXT,
    Comment_author MEDIUMTEXT,
    Comment_posted_date VARCHAR(30),
    Like_count INT,
    Reply_count INT
    )""")

# CALLING THIS FUNCTION CREATES AN MYSQL TABLES
create_mysql_tables()


# HOME PAGE
if selected == "Home":

# Column 1
 col1, col2 = st.columns(2, gap='medium')
 col1.markdown("## :blue[Title]: YouTube Data Harvesting")
 col1.markdown("## :blue[Technologies] : Python, MongoDB, YouTube Data API, MySQL, Streamlit")
 col1.markdown("## :blue[Overview] : Retrieve YouTube channel data, store in MongoDB, migrate to MYSQL, query and display in Streamlit.")

# Column 2
 col2.markdown("#    ")
 col2.markdown("#    ")
 col2.markdown("#    ")



# EXTRACT AND TRANSFORM PAGE
if selected == "Extract-migrate & Transfer":
    tab1, tab2 = st.tabs(["$/huge 💾 EXTRACT-MIGRATE $", "$/huge 🔁TRANSFER $"])
    
    # EXTRACT TAB
    with tab1:
        st.write("### Enter YouTube Channel_ID below:")
        ch_id = st.text_input("Retrive the channel details using channel_id and store in :green[MONGODB]")

        if ch_id and st.button("Extract Data"):
            logging.info(f"Extracting data for channel IDs: {ch_id}")
            ch_details = get_channel_details(ch_id)
            st.write(f'#### Extracted data from :green["{ch_details[0]["Channel_name"]}"] channel')
            st.table(ch_details)

        if st.button("Migrate to MongoDB"):
            with st.spinner('Please Wait for a sec....'):
                ch_details = get_channel_details(ch_id)
                v_ids = get_channel_videos(ch_id)
                vid_details = get_video_details(v_ids)

                def comments():
                    com_d = []
                    for i in v_ids:
                        com_d += get_comments_details(i)
                    return com_d

                comm_details = comments()

                collections1 = db.channel_details
                collections1.insert_many(ch_details)

                collections2 = db.video_details
                collections2.insert_many(vid_details)

                collections3 = db.comments_details
                collections3.insert_many(comm_details)

                logging.info("Migrate to MongoDB successful")

                st.success("Migrate to MongoDB successful !!",icon ="😃" )
                st.balloons()

    # TRANSFORM TAB
    with tab2:     
        st.markdown("#   ")
        st.markdown("### :orange[Select a channel to begin Transfer to SQL]")
        st.write("Transfer Data from :green[MONGODB] to :blue[MYSQL]")

        ch_names = channel_names()
        user_inp = st.selectbox("Select channel", options=ch_names)

        def table_for_added_channel_to_sql():
            query = ('select * from Channel_table')
            mycursor.execute(query)
            tabel = mycursor.fetchall()

            i = [i for i in range(1, len(tabel)+1)]
            tabel = pd.DataFrame(tabel, columns=mycursor.column_names, index=i)
            tabel = tabel[["Channel_name", "Subscribers", "Views"]]
            st.markdown("### channels migrated to mysql")
            st.dataframe(tabel)



    # FUNCTION FOR MIGRATING OF CHANNEL DETAILS FROM MONGODB TO MYSQL TABLE
        def insert_into_channel_details():
            collections = db.channel_details
            query = """INSERT INTO channel_details VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""
            for i in collections.find({"Channel_name" :user_inp},{'_id':0,'Thumbnail':0}):
                a=i["Description"]
                i["Description"]=a[:30]
                t=tuple(i.values())
                mycursor.execute(query,t)
            mydb.commit()    
   

# FUNCTION FOR MIGRATION OF VIDEOS FROM MONGODB TO MYSQL TABLE
        def insert_into_video_details():
            collections1= db.video_details
            query1 = """INSERT INTO video_details VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            for i in collections1.find({"Channel_name" :user_inp},{"_id":0}):
                t=tuple(i.values())
                mycursor.execute(query1,t)
            mydb.commit()     

# FUNCTION FOR MIGRATION OF COMMENTS FROM MONGODB TO MYSQL TABLE
        def insert_into_comments():
            collections1=db.video_details
            collections2=db.comments_details
            query="""INSERT INTO comments VALUES(%s,%s,%s,%s,%s,%s,%s)"""
            for vid in collections1.find({'Channel_name':user_inp},{'_id':0}):
              for i in collections2.find({'Video_id':vid["Video_id"]},{'_id':0}):
                 mycursor.execute(query,tuple(i.values()))
            mydb.commit()  


        if st.button("Submit"):
            try:
                insert_into_channel_details()
                st.success("Transformation of channel_details got successful !!",icon="✅")
            except sql.Error as err:
                print(f"MYSQL error (channel_details): {err}")
                st.error("An error occured while tranforming channel_details to mysql.")
            except Exception as e:
                print(f"An error occured of channel_Details: {e}")
                st.error("An error occured while transforming channel_details to MYSQL.")


            try:
                #insert into video
                insert_into_video_details()
                st.success("Transformation of video_details got successful !!",icon="✅")
            except sql.Error as err:
                print(f"MYSQL error (videos_details): {err}")
                st.error("An error occured while tranforming video_details to mysql.")

            try:
                #insert into comment
                insert_into_comments()
                st.success("Transformation of comments got successful !!",icon="✅")
            except sql.Error as err:
                print(f"MYSQL error (comments_details): {err}")
                st.error("An error occured while tranforming comments_details to mysql.")



# VIEW PAGE
if selected == "View":
    
    st.write("## :red[Select any question to get Insights]")
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
        mycursor.execute("""SELECT Title AS Video_Title, channel_name AS Channel_Name
                            FROM video_details
                            ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        mycursor.execute("""SELECT Channel_name AS Channel_Name, COUNT(Video_id) AS Total_Videos
                            FROM video_details GROUP BY Channel_name
                            ORDER BY Total_videos DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Number of videos in each channel :]")
        #st.bar_chart(df,x= mycursor.column_names[0],y= mycursor.column_names[1])
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, title AS Video_Title, views AS Views 
                            FROM video_details
                            ORDER BY views DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most viewed videos :]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT a.video_id AS Video_id, a.title AS Video_Title, b.Total_Comments
                            FROM video_details AS a
                            LEFT JOIN (SELECT video_id,COUNT(comment_id) AS Total_Comments
                            FROM comments GROUP BY video_id) AS b
                            ON a.video_id = b.video_id
                            ORDER BY b.Total_Comments DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
   
    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,title AS Title,likes AS Likes_Count 
                            FROM video_details
                            ORDER BY likes DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most liked videos :]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
    
    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT title AS Title, likes AS Likes_Count
                        FROM video_details
                        ORDER BY likes DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)

    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, views AS Views
                            FROM channel_details
                            ORDER BY views DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Channels vs Views :]")
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)

    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        mycursor.execute("""SELECT channel_name AS Channel_Name
                            FROM video_details
                            WHERE published_date LIKE '2022%'
                            GROUP BY channel_name
                            ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
    
    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
         mycursor.execute("""SELECT 
                          channel_name AS Channel_Name,
                          AVG(
                          COALESCE(NULLIF(CONVERT(SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'M', 1), 'PT', -1), UNSIGNED INTEGER) * 60, 0), 0) +
                          COALESCE(NULLIF(CONVERT(SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'M', -1), UNSIGNED INTEGER), 0), 0)
                          )/60 AS "Average_Video_Duration (mins)"
                          FROM 
                          video_details
                          GROUP BY 
                          channel_name
                          ORDER BY 
                          "Average_Video_Duration (mins)" DESC;
                          """)
         df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
         st.write(df)
         st.write("### :green[Avg video duration for channels :]")
         fig = px.bar(df,
                 x=mycursor.column_names[0],
                 y=mycursor.column_names[1],
                 orientation='v',
                 color=mycursor.column_names[0]
                )
         st.plotly_chart(fig,use_container_width=True)

    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,video_id AS Video_ID,comments AS Comments
                            FROM video_details
                            ORDER BY comments DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Videos with most comments :]")
        fig = px.bar(df,
                     x=mycursor.column_names[1],
                     y=mycursor.column_names[2],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
          
    
     


        