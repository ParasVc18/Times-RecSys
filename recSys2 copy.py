import os
import pymysql
import pandas as pd
from numpy.random import choice

host = os.getenv('MYSQL_HOST')
port = os.getenv('MYSQL_PORT')
user = os.getenv('MYSQL_USER')
password = os.getenv('MYSQL_PASSWORD')
database = os.getenv('MYSQL_DATABASE')

conn = pymysql.connect(
    host=host,
    port=int(3306),
    user="root",
    passwd=password,
    db="rec1",
    charset='utf8mb4')

c=conn.cursor()
c.execute("select * from User")
users = c.fetchall()

dict_df=pd.DataFrame(columns=['lead','follow','freq'])

for u in users: 
    chain=u[1]
    chain=chain.split(',')
    i=0
    for i in range (0,len(chain)-1):
        x={'lead': chain[i], 'follow':chain[i+1]}
        dict_df=dict_df.append(x,ignore_index=True)
        i=i+1 

dict_df['freq']= dict_df.groupby(by=['lead','follow'])['lead','follow'].transform('count').copy()    
dict_df = dict_df.drop_duplicates()
pivot_df = dict_df.pivot(index = 'lead', columns= 'follow', values='freq')

sum_words = pivot_df.sum(axis=1)
pivot_df = pivot_df.apply(lambda x: x/sum_words)

def make_rec(start):
    prod= start
    rec=[prod]
    while len(rec) < 5:
        next_prod = choice(a = list(pivot_df.columns), p = (pivot_df.iloc[pivot_df.index ==prod].fillna(0).values)[0])
        rec.append(next_prod)
        rec=list(dict.fromkeys(rec))
        prod=next_prod
    rec = ' '.join(rec)
    return rec

rec = make_rec('6')
print (rec)