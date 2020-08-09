import psycopg2
from textblob import TextBlob

try:
    conn = psycopg2.connect("dbname='postgres'")
except:
    print("Can't connect to PSQL!")

cursor = conn.cursor()
cursor.execute("""SELECT id, experience_text from erowid.experiences""")
rows = cursor.fetchall()

for row in rows:
    id = row[0]
    text = TextBlob(row[1])
    sentimentPolarity = text.sentiment.polarity
    sentimentSubjectivity = text.sentiment.subjectivity

    cursor.execute(
        """UPDATE erowid.experiences SET sentiment_polarity = %s, sentiment_subjectivity = %s WHERE id = %s""",
                                    (sentimentPolarity, sentimentSubjectivity, id))

conn.commit()
cursor.close()
conn.close()
