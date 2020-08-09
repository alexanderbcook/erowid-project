import requests
import xml.etree.ElementTree as ET
import psycopg2
import time
import sys

headers = {
    'User-Agent': 'Erowid Sentiment Analysis Project'
    }

try:
    conn = psycopg2.connect("dbname='postgres'")
except:
    print("Can't connect to PSQL!")

cursor = conn.cursor()

cursor.execute("CREATE TABLE IF NOT EXISTS erowid.experiences (id INT PRIMARY KEY, principal_substance VARCHAR, principal_substance_id INT, list_number INT, title VARCHAR, author VARCHAR, substance_string VARCHAR, body_weight VARCHAR, gender VARCHAR, published_date DATE, submitted_date DATE, experience_year VARCHAR, intensity VARCHAR, primary_category_id INT, substance_id_list VARCHAR, category_id_list VARCHAR, published_rating VARCHAR, body_changes VARCHAR, experience_text VARCHAR, sentiment_polarity REAL, sentiment_subjectivity REAL);")

experienceAlreadyCaptured = []
cursor.execute("SELECT id  FROM erowid.experiences");
for value in cursor.fetchall():
    experienceAlreadyCaptured.append(str(value[0]))

cursor.execute("SELECT id, name FROM erowid.substances ORDER BY id DESC;")

for value in cursor.fetchall():
    substanceId = value[0]
    substanceName = value[1]

    experienceListXmlComplete = requests.get('https://erowid.org/experiences/research/exp_api.php?api_code=SentimentAnalysis2020a&a=experience_list&substance_id='+str(substanceId)+'&format=xml', headers=headers)
    experienceListXmlSplit = experienceListXmlComplete.text.split('</request-parameters>')[1]
    root = ET.fromstring(experienceListXmlSplit)
    
    for child in root.findall('experience-id-list-result'):

        experienceArray = child.text.split(', ')

        for experienceId in experienceArray:
            if not str(experienceId) in experienceAlreadyCaptured:

                experienceRequest = requests.get('https://erowid.org/experiences/research/exp_api.php?api_code=SentimentAnalysis2020a&a=experience_data&experience_id='+experienceId+'&format=xml', headers=headers)
                print('Fetched '+substanceName+' experience from URL: https://erowid.org/experiences/research/exp_api.php?api_code=SentimentAnalysis2020a&a=experience_data&experience_id='+experienceId+'&format=xml')

                experienceXmlData = experienceRequest.text.split('</request-parameters>')[1]
                try:
                    root = ET.fromstring(experienceXmlData.replace('&',''))
                except:
                    pass
                try:
                    for child in root.findall('experience'):
                        cursor.execute("INSERT INTO erowid.experiences (id, principal_substance, principal_substance_id, list_number, title, author, substance_string, body_weight, gender, published_date, submitted_date, experience_year, intensity, primary_category_id, substance_id_list, category_id_list, published_rating, body_changes, experience_text) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",(child.find('id').text, substanceName, substanceId, child.find('list-number').text, child.find('title').text, child.find('author').text, child.find('substance-string').text, child.find('body-weight').text,child.find('gender').text,child.find('published-date').text,child.find('submitted-date').text,child.find('experience-year').text, child.find('intensity').text,child.find('primary-category-id').text,child.find('substance-id-list').text,child.find('category-id-list').text,child.find('published-rating').text,child.find('body-changes').text,child.find('experience-text').text))
                        conn.commit()
                        print('Record uploaded!')
                        time.sleep(.33)
                except:
                    pass


cursor.close()
conn.close()