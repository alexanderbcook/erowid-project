import psycopg2
import logging
from textblob import TextBlob
import config
import ast

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

conn = psycopg2.connect(config.conn_string)
print ('Successfully connected to database')

cur = conn.cursor()
cur.execute("""SELECT id, body, substances from erowid.main""")
rows = cur.fetchall()

encounteredSubstances = []
substanceTitles = []
formatTitles = []

for row in rows:

    substances = ast.literal_eval(row[2])
    print substances

    if len(substances) == 1 and substances[0] not in encounteredSubstances:
        encounteredSubstances.append(substances[0])
        substanceTitles.append(substances[0].lower())

def formatTitle(title, array):

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

    array.append(title)
    print array
    return array

formattedTitles = []
for title in substanceTitles:
    formatTitle(title, formattedTitles)

for title in formattedTitles:

    cur.execute('CREATE TABLE IF NOT EXISTS erowid.{0} (category varchar, gender varchar, drug varchar,views varchar, substances varchar, body varchar, weight varchar, title varchar, year varchar, user_name varchar, doses varchar, id varchar, date varchar, sentiment double precision, good_sentences varchar, bad_sentences varchar)'.format(title))
    conn.commit()
cur.close()
conn.close()



