import requests
import xml.etree.ElementTree as ET
import psycopg2
import time

categoryRequest = requests.get('https://erowid.org/experiences/research/exp_api.php?api_code=SentimentAnalysis2020a&a=category_list&format=xml')

try:
    conn = psycopg2.connect("dbname='postgres'")
except:
    print("Can't connect to PSQL!")

cursor = conn.cursor()
cursor.execute("CREATE TABLE IF NOT EXISTS erowid.categories (id INT PRIMARY KEY, name VARCHAR, type VARCHAR, primary_category VARCHAR)")    

headers = {
    'User-Agent': 'Erowid Sentiment Analysis Project (https://databyalex.com/erowid/sentiment)'
    }

xmlData = categoryRequest.text
root = ET.fromstring(xmlData.replace('&',''))
 
for child in root.findall('category'):
    cursor.execute("INSERT INTO erowid.categories (id, name, type, primary_category) VALUES (%s, %s, %s, %s)",(child.find('id').text, child.find('name').text, child.find('type').text, child.find('primary-category').text))
    conn.commit()

cursor.close()
conn.close()
