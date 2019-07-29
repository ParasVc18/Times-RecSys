import os
import pymysql
import re
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import math

host = os.getenv('MYSQL_HOST')
port = os.getenv('MYSQL_PORT')
user = os.getenv('MYSQL_USER')
password = os.getenv('MYSQL_PASSWORD')
database = os.getenv('MYSQL_DATABASE')

def count_words(s):
    count=0
    words=word_tokenize(s)
    for w in words:
        count+=1
    return count

def create_a_freq_dict(s):
    freqDict_list=[]
    for a in s:
        freq_dict_t={}
        freq_dict_st={}
        freq_dict_b={}
        words=word_tokenize(a["title"])
        for word in words:
            if word not in stopwords.words():
                word=word.lower()
                if word in freq_dict_t:
                    freq_dict_t[word]+=1
                else:
                    freq_dict_t[word]=1
        words=word_tokenize(a["sub_title"])
        for word in words:
            if word not in stopwords.words():
                word=word.lower()
                if word in freq_dict_st:
                    freq_dict_st[word]+=1
                else:
                    freq_dict_st[word]=1
        words=word_tokenize(a["body"])
        for word in words:
            if word not in stopwords.words():
                word=word.lower()
                if word in freq_dict_b:
                    freq_dict_b[word]+=1
                else:
                    freq_dict_b[word]=1
        freqDict_list.append({
                "id":a["id"],
                "freq_dict_t":freq_dict_t,
                "freq_dict_st":freq_dict_st,
                "freq_dict_b":freq_dict_b
                })
    return freqDict_list

def create_a_new_freq_dict(a):
    freq_dict_t={}
    freq_dict_st={}
    freq_dict_b={}
    words=word_tokenize(a["title"])
    for word in words:
        if word not in stopwords.words():
            word=word.lower()
            if word in freq_dict_t:
                freq_dict_t[word]+=1
            else:
                freq_dict_t[word]=1
    words=word_tokenize(a["sub_title"])
    for word in words:
        if word not in stopwords.words():
            word=word.lower()
            if word in freq_dict_st:
                freq_dict_st[word]+=1
            else:
                freq_dict_st[word]=1
    words=word_tokenize(a["body"])
    for word in words:
        if word not in stopwords.words():
            word=word.lower()
            if word in freq_dict_b:
                freq_dict_b[word]+=1
            else:
                freq_dict_b[word]=1
    return {"id":a["id"],
            "freq_dict_t":freq_dict_t,
            "freq_dict_st":freq_dict_st,
            "freq_dict_b":freq_dict_b}

def create_p_freq_dict(s):
    freqDict_list=[]
    for p in s:
        freq_dict_d={}
        freq_dict_type={}
        words=word_tokenize(p["description"])
        for word in words:
            if word not in stopwords.words():
                word=word.lower()
                if word in freq_dict_d:
                    freq_dict_d[word]+=1
                else:
                    freq_dict_d[word]=1
        words=word_tokenize(p["type"])
        for word in words:
            if word not in stopwords.words():
                word=word.lower()
                if word in freq_dict_type:
                    freq_dict_type[word]+=1
                else:
                    freq_dict_type[word]=1
        freqDict_list.append({
                "id":p["id"],
                "freq_dict_d":freq_dict_d,
                "freq_dict_type":freq_dict_type
                })
    return freqDict_list

def create_p_new_freq_dict(p):
    freq_dict_d={}
    freq_dict_type={}
    words=word_tokenize(p["description"])
    for word in words:
        if word not in stopwords.words():
            word=word.lower()
            if word in freq_dict_d:
                freq_dict_d[word]+=1
            else:
                freq_dict_d[word]=1
    words=word_tokenize(p["type"])
    for word in words:
        if word not in stopwords.words():
            word=word.lower()
            if word in freq_dict_type:
                freq_dict_type[word]+=1
            else:
                freq_dict_type[word]=1
    return {"id":p["id"],
            "freq_dict_d":freq_dict_d,
            "freq_dict_type":freq_dict_type
            }

conn = pymysql.connect(
    host=host,
    port=int(3306),
    user="root",
    passwd=password,
    db="rec1",
    charset='utf8mb4')

c=conn.cursor()
c.execute("select * from article")
articles = c.fetchall()
art=[]
for a in articles:
    art.append({
        "id":a[4],
        "title":(re.sub('\s+',' ',re.sub('[^\w\s]','',str(a[0])))).strip(),
        "tlen":count_words((re.sub('\s+',' ',re.sub('[^\w\s]','',str(a[0])))).strip()),
        "sub_title":(re.sub('\s+',' ',re.sub('[^\w\s]','',str(a[1])))).strip(),
        "stlen":count_words((re.sub('\s+',' ',re.sub('[^\w\s]','',str(a[1])))).strip()),
        "body":(re.sub('\s+',' ',re.sub('[^\w\s]','',str(a[3])))).strip(),
        "blen":count_words((re.sub('\s+',' ',re.sub('[^\w\s]','',str(a[3])))).strip()),
        "type":a[2]
        })
    
c.execute("select * from product")
products=c.fetchall()
prod=[]
for p in products:
    prod.append({
    "id":p[6],
    "name":(re.sub('\s+',' ',re.sub('[^\w\s]','',str(p[0])))).strip(),
    "company":p[1],
    "description":(re.sub('\s+',' ',re.sub('[^\w\s]','',str(p[2])))).strip(),
    "dlen":count_words((re.sub('\s+',' ',re.sub('[^\w\s]','',str(p[2])))).strip()),
    "type":p[3],
    "tlen": count_words(str(p[3])),
    "category":p[4]
    })
    
afd=create_a_freq_dict(art)  
pfd=create_p_freq_dict(prod)

def cosine_sim(dic1,dic2):
    num=0
    dena=0
    for key1,val1 in dic1.items():
        num += val1*dic2.get(key1,0.0)
        dena += val1*val1
    denb = 0
    for val2 in dic2.values():
        denb += val2*val2
    return num/math.sqrt(dena*denb)

def total_sim(afd,pfd):
    sim=[]
    for a in afd:
        for p in pfd:
            sim.append({
                    "aid":a["id"],
                    "pid":p["id"],
                    "sim":(((6*cosine_sim(a["freq_dict_t"],p["freq_dict_d"]))+(4*cosine_sim(a["freq_dict_st"],p["freq_dict_d"]))+(2*cosine_sim(a["freq_dict_b"],p["freq_dict_d"])+(5*cosine_sim(a["freq_dict_t"],p["freq_dict_type"]))+(3*cosine_sim(a["freq_dict_st"],p["freq_dict_type"]))+(cosine_sim(a["freq_dict_b"],p["freq_dict_type"]))))/21)
                    })
            
    return sim

def new_a_sim(new_afd,pfd,sim):
    for p in pfd:
        sim.append({
                "aid":new_afd["id"],
                "pid":p["id"],
                "sim":(((6*cosine_sim(new_afd["freq_dict_t"],p["freq_dict_d"]))+(4*cosine_sim(new_afd["freq_dict_st"],p["freq_dict_d"]))+(2*cosine_sim(new_afd["freq_dict_b"],p["freq_dict_d"])+(5*cosine_sim(new_afd["freq_dict_t"],p["freq_dict_type"]))+(3*cosine_sim(new_afd["freq_dict_st"],p["freq_dict_type"]))+(cosine_sim(new_afd["freq_dict_b"],p["freq_dict_type"]))))/21)
                })
    return sim

def new_p_sim(afd,new_pfd,sim,lst):
    i=0
    for a in afd:
        new_sim=(((6*cosine_sim(a["freq_dict_t"],new_pfd["freq_dict_d"]))+(4*cosine_sim(a["freq_dict_st"],new_pfd["freq_dict_d"]))+(2*cosine_sim(a["freq_dict_b"],new_pfd["freq_dict_d"])+(5*cosine_sim(a["freq_dict_t"],new_pfd["freq_dict_type"]))+(3*cosine_sim(a["freq_dict_st"],new_pfd["freq_dict_type"]))+(cosine_sim(a["freq_dict_b"],new_pfd["freq_dict_type"]))))/21)
        if new_sim>min(l["sim"]for l in lst[i:i+2]):
            if new_sim>max(l["sim"]for l in lst[i:i+2]):
                lst[i]["pid"]=new_pfd["id"]
                lst[i]["sim"]=new_sim
            elif max(l["sim"]for l in lst[i:i+2])>new_sim>lst[i+1]["sim"]:
                lst[i+1]["pid"]=new_pfd["id"]
                lst[i+1]["sim"]=new_sim
            else:
                lst[i+2]["pid"]=new_pfd["id"]
                lst[i+2]["sim"]=new_sim
        sim.append({
                "aid":a["id"],
                "pid":new_pfd["id"],
                "sim":new_sim
                })
        i=i+3
    return sim,lst

new_a = {
        "id": 102,
        "title": "Do this that with lorem ipsum",
        "sub_title": "1.This 2.That",
        "body": "The quick brown fox bla bla",
        "type": "Entertainment"
        }
new_p = {
        "id":52,
        "name": "Absolutely nothing",
        "description": "Kartik Aaryan & Sidharth Malhotra Show Us That There Can Never Be Too Many Holes In A T-shirt",
        "type": "Ripped t-shirts",
        "category": "Accessories"
        }

sim=total_sim(afd,pfd) #SIMILARITY COMPUTATION FOR MAJORITY ARTICLES & PRODUCTS

if new_a["id"] not in [a["id"] for a in art]:
    new_afd= create_a_new_freq_dict(new_a) #Call these three when NEW ARTICLE
    afd.append(new_afd)
    sim=new_a_sim(new_afd,pfd,sim)

prods_rel_to_a=[]

for a in afd:
    lst=[]
    for s in sim: 
        if s["aid"]==a["id"]:
            lst.append(s)    
    lst = sorted(lst, key = lambda i: i["sim"],reverse=True)[:3] 
    prods_rel_to_a=prods_rel_to_a + lst

if new_p["id"] not in [p["id"] for p in prod]:
    new_pfd= create_p_new_freq_dict(new_p) #Call these three when NEW PRODUCT
    pfd.append(new_pfd)
    sim,prods_rel_to_a=new_p_sim(afd,new_pfd,sim,prods_rel_to_a)

print(prods_rel_to_a)
