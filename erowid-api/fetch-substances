import requests
import xml.etree.ElementTree as ET
import psycopg2
import time

substanceRequest = requests.get('https://erowid.org/experiences/research/exp_api.php?api_code=SentimentAnalysis2020a&a=substance_list&format=xml')

try:
    conn = psycopg2.connect("dbname='postgres'")
except:
    print("Can't connect to PSQL!")

cursor = conn.cursor()
cursor.execute("CREATE TABLE IF NOT EXISTS erowid.substances (id INT PRIMARY KEY, name VARCHAR, non_substance_flag VARCHAR, experience_count INT, experience_count_with_child_substances INT)")    

headers = {
    'User-Agent': 'Erowid Sentiment Analysis Project (https://databyalex.com/erowid/sentiment)'
    }

xmlData = substanceRequest.text
root = ET.fromsroot = ET.fromstring(xmlData.replace('&',''))
 
for child in root.findall('substance'):
    cursor.execute("INSERT INTO erowid.substances (id, name, non_substance_flag, experience_count, experience_count_with_child_substances) VALUES (%s, %s, %s, %s, %s)",(child.find('id').text, child.find('name').text, child.find('non-substance-flag').text, child.find('experience-count').text, child.find('experience-count-with-child-substances').text))
    conn.commit()

cursor.close()
conn.close()
