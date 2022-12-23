# -*- coding: utf-8 -*-
"""
Created on Thu Dec 22 10:28:41 2022

@author: hp
"""

# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import configparser
import time
import datetime
import pandas as pd;
import re;
from bs4 import BeautifulSoup
import requests
import pandas as pd;
import os


working_directory=os.getcwd()
config_file="web_scraper.ini"
output_file_name="new.csv"
config_output_file_path=working_directory+"\\"+config_file
output_file_path=working_directory+"\\"+output_file_name
INTERVAL_DELAY_SECONDS=3
SENT_STATUS='sent'
UNSENT_STATUS='unsent'
ERROR_STATUS='error'
EXPIRY_DAYS=60
MAX_ATTEMPT=5
topic_repos_dict={
    'topic':[],
    'newspaper':[],
    'url':[],
    'time':[],
    'status':[]
}
#mandatory_words=['customers','cbic','seizure','seizes','Customs_137'];
#search_string="dri seizure "

current_time_epoch=time.time()
current_seconds=(current_time_epoch)%3600
current_hour_time_epoch=current_time_epoch-current_seconds

def isValidTopic(topic_heading_newspaper):
    topic_heading_newspaper_list=re.split('[ \'()-+%$#@!^*"`|\/]',topic_heading_newspaper)
    check=False
    for i in range(len(mandatory_words)):
        temp=True;
        current_val_list=mandatory_words[i].split("_")
        for j in range(len(current_val_list)):
            temp=temp and (current_val_list[j] in topic_heading_newspaper_list)
        check= check or temp
    return check

def isNewTopic(news_time,current_hour_time_epoch):
    date_time_current = datetime. datetime. fromtimestamp(current_hour_time_epoch)
    news_time_epoch=time.mktime(datetime.datetime.strptime(news_time, "%Y-%m-%dT%H:%M:%SZ").timetuple())
    date_time_news=datetime. datetime. fromtimestamp(news_time_epoch)
    difference_epoch_seconds=(date_time_current-date_time_news).seconds + (date_time_current-date_time_news).days*3600*24
    if((difference_epoch_seconds)<=3600*24*EXPIRY_DAYS): return True
    else: return False

def isSimilar(topic1, topic2):
    return False

def isSame(existing_row,new_row):
    if (new_row[0]==existing_row[0]):
        return True
    elif ((isSimilar(new_row[0],existing_row[0]))==True):
        return True
    return False

def telegram_bot_sendtext(bot_message):
    
    bot_token = '5505231045:AAHOjxAlRjH2D45jBWwSsGMkk19VQJ-WJMM'
    bot_chatID = '1155691401'
    channel_name='web_scraper'
    channel_id='-1001635625848'
    #print(bot_message)
    send_url='https://api.telegram.org/bot'+bot_token+'/sendMessage?chat_id='+channel_id+'&parse_mode=Markdown&text='+bot_message
    return requests.post(send_url)



config = configparser.ConfigParser()
config.read(config_output_file_path);
for each_section in config.sections():
    search_string=config.get(each_section,'search_string')
    mandatory_words=config.get(each_section,'mandatory_words').split(",")
    search_string_for_google=search_string.replace(" ","%20")
    topic_url='https://news.google.com/search?q='+search_string_for_google+'&hl=en-IN&gl=IN&ceid=IN%3Aen'
    print("URL to analyze: " +topic_url)
    for i in range(MAX_ATTEMPT):
        try:
            response=requests.get(topic_url)
            break;
        except:
            print("Error obtaining data from URL: "+topic_url)
    
    #time.sleep(10)
    response.status_code
    page_contents=response.text

    
    doc= BeautifulSoup(page_contents, 'html.parser')
    a_tags=doc.find_all('a')
    selection_class='DY5T1d RZIKme';
    a_tags=doc.find_all('a', 
                        { 'class':selection_class})   
    time_class='WW6dff uQIVzc Sksgp slhocf';
    time_tags=doc.find_all('time', 
                        { 'class':time_class})
    print("total items retrieved from google news: "+str(len(a_tags)))
    for i in range(0,len(a_tags)):
        topic_heading_newspaper=a_tags[i].text
        #print(topic_heading_newspaper)
        if(isValidTopic(topic_heading_newspaper)==False):
            continue;
        if(isNewTopic(time_tags[i]['datetime'],current_hour_time_epoch)==False):
            continue;
        a_tags[i]['href']
        suffix=a_tags[i]['href'];
        base_url='https://news.google.com/';
        url=suffix.replace ("./", base_url,1)
        #print(1)
        #time.sleep(1)
        try:
            newspaper_article=requests.get(url);
        except:
            print("error in obtaining data for url:"+url)
            continue
        #print(2)
        #time.sleep(1)
        newspaper_article.status_code
        newspaper_article_contents=newspaper_article.text
        newspaper_article_url=newspaper_article.url
        newspaper_agency=(newspaper_article_url.split("//")[1].split("/")[0])
        topic_repos_dict['topic'].append(topic_heading_newspaper);
        topic_repos_dict['newspaper'].append(newspaper_agency);
        topic_repos_dict['url'].append(newspaper_article_url);
        topic_repos_dict['time'].append(time_tags[i]['datetime']);
        topic_repos_dict['status'].append(UNSENT_STATUS);
    print("*****analyzed all the items on URL..now sleeping for "+str(INTERVAL_DELAY_SECONDS)+" seconds*****")    
    time.sleep(INTERVAL_DELAY_SECONDS)
    
#-------------------retrieved all the topics from google news-------------
print("*****web_scraping successful...now recording it in the file*****")
#-------------------now recording it in the file-------------------------
    
topic_new_df=pd.DataFrame(topic_repos_dict)
if os.path.isfile(output_file_path)==False: 
    print("file does not exist...creating it")
    with open(output_file_path, 'w') as creating_new_csv_file: 
        pass 
    print("Empty File Created Successfully")
if os.stat(output_file_path).st_size==0:
    print("file is empty")
    topic_prev_df=pd.DataFrame()
else:
    topic_prev_df=pd.read_csv(output_file_path,header=None,index_col=False)
to_be_deleted=[];
for index2, existing_row in topic_prev_df.iterrows():
    row_num=-1
    for index1, new_row in topic_new_df.iterrows():
        row_num=row_num+1;
        a=isSame(existing_row,new_row)
        if(a)==True:
            to_be_deleted.append(row_num)
topic_new_df.drop(to_be_deleted, inplace=True)
topic_new_df.index=pd.RangeIndex(start=len(topic_prev_df.index), stop=len(topic_prev_df.index)+len(topic_new_df.index), step=1)
topic_new_df.to_csv(output_file_path,mode='a',encoding='utf-8',header=False,index=False)

print("following new records added to the file:...")
print(topic_new_df)
    
print("*****data stored in file successfully*****")



#-------------------scraping and recording operation successful---------------


#-------------------sharing and purging older records-------------------------

topic_prev_df=pd.read_csv(output_file_path,header=None)

to_be_deleted=[];
row_num=-1;
shared=0;
for index2, existing_row in topic_prev_df.iterrows():
    row_num=row_num+1
    if(isNewTopic(existing_row[3],current_hour_time_epoch)==False):
            to_be_deleted.append(row_num)
            continue;
    if existing_row[4]!=SENT_STATUS:
        response=telegram_bot_sendtext(existing_row[2])
        shared=shared+1
        #print("sending to telegram:"+existing_row[2])
        if(response.status_code==200):
            topic_prev_df.at[index2,4]=SENT_STATUS
            print("successfully sent to telegram: "+existing_row[0])
        else:
            topic_prev_df.at[index2,4]=ERROR_STATUS
            print("error sending url:"+existing_row[2])
        #print(existing_row[0]+" status")    
print("*****shared total "+str(shared)+" new records on telegram successfully*****")

topic_prev_df.drop(to_be_deleted, inplace=True)
print("purged total "+str(len(to_be_deleted))+" older records.")
topic_prev_df.index=pd.RangeIndex(start=0, stop=len(topic_prev_df.index), step=1)
topic_prev_df.to_csv(output_file_path,mode='w+',encoding='utf-8',header=0,index=False)

print("the updated file after telegram sharing is as following:")
print(topic_prev_df)
    
print("*************operation completed successfully*******************")
        
        

