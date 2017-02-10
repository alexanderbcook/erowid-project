import psycopg2
from psycopg2.extensions import AsIs
import logging
from textblob import TextBlob
import config
import ast

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

try:
    conn = psycopg2.connect(config.conn_string)
    logging.debug('Successfully connected to database')
except:
    logging.debug('Connection to database failed. Check configuration settings.')

cur = conn.cursor()
cur.execute("""SELECT category, gender, drug, views, drug, substances, body, weight, title, year, user_name, doses, id, date from erowid.main""")
rows = cur.fetchall()

letters = ['A','B','C','D','E','F','G','H','I','J','K','L','M'
           'N','O','P','Q','R','S','T','U','V','W','X','Y','Z']
punctuation = ['?','.','!']
unicode = '\u'

def formatTitle(title):

    for ch in [' - ','. ',' ','-',',',"'",'/','.']:
        if ch in title:
            title = title.replace(ch,'_')
            title = title.rstrip('.')
            title = title.replace('2','two_')
            title = title.replace('7','seven_')
            title = title.replace('8','eight_')
            title = title.replace('9','nine_')
            title = title.replace('0','zero_')
            title = title.replace('5', 'five_')
            title = title.replace('4','four_')
            title = title.replace('6','six_')
            title = title.replace('3','three_')
            title = title.replace('1','one_')

    return title

for row in rows:
    category = row[0]
    gender = row[1]
    views = row[2]
    drug = row[3]
    substances = ast.literal_eval(row[5])
    body_text = row[6]
    text = TextBlob(row[6])
    weight = row[7]
    title = row[8]
    year = row[9]
    user = row[10]
    doses = row[11]
    id = row[12]
    published = row[13]
    aggregateSentiment = text.sentiment.polarity

    goodSentence = None
    badSentence = None

    if len(substances) == 1:
        sentences = text.sentences
        sentiment = text.sentiment.polarity

        for sentence in sentences:
            if len(sentence) < 80  and len(sentence) > 60 and sentence[0] in letters and sentence[len(sentence) - 1] in punctuation and unicode not in sentence:
                text = TextBlob(str(sentence))
                if text.sentiment.polarity > .9 and text.sentiment.subjectivity > .7:
                    goodSentence = sentence
                if text.sentiment.polarity < -.9 and text.sentiment.subjectivity > .7:
                    badSentence = sentence

        cur.execute(
            """INSERT INTO erowid.%s (category, gender, drug, views, weight,
                                      title, year, user_name, id, date, sentiment, good_sentences, bad_sentences)
                VALUES (%s, %s, %s, %s, %s, %s ,%s , %s, %s, %s , %s, %s, %s);""",
                                    (AsIs(formatTitle(substances[0].lower())), category, gender, drug, views.replace(',',''), weight,
                                     title, year, user, id, published, aggregateSentiment, str(goodSentence), str(badSentence)))

        conn.commit()

cur.close()
conn.close()