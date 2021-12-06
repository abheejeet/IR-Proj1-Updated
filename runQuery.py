# -*- coding: utf-8 -*-

import json
import requests
from flask import Flask
from googletrans import Translator
app = Flask(__name__)


AWS_PUBLIC_IP="3.145.29.188"
SOLR_CORE="IRF21P4001"


@app.route('/')
def hello():
    return 'Hello, World!'


URL="http://"+AWS_PUBLIC_IP+":8983/solr/"+SOLR_CORE+"/select"

def getQueryResult_BM25(fields,query):
    print(query)
    # fields='text_en^2.5 + text_ru^2.5 + text_de^2.5 + tweet_hashtags^1'
    PARAMS = {'defType':'dismax','qf':fields,'q':query,'fl':'id,score,tweet_text','wt':'json','indent':'true','rows':20}

    r = requests.get(url=URL, params=PARAMS)
    data = r.json()
    return data


def detectLang(input):
    translator = Translator()
    result=translator.detect(input)
    return result.lang


def convertToOtherLang(text,inputLang,outputLang):

    if(inputLang==outputLang):
        return text
    else:
        translator = Translator()
        result= translator.translate(text,outputLang,inputLang)
        return result.text

#
# def convertLang():

def createQuery(text):
    detectedLang=detectLang(text)

    if(detectedLang not in ['en','es','hi']):
        fields='text_en^2.5 + text_hi^2.5 + text_es^2.5 + hashtags^1.5 + mentions^1.2'
        result=getQueryResult_BM25(fields,text)
        return result
    else:
        result=[]
        for lang in ['en','hi','es']:
            if lang=='en':
                fields = 'text_en^2.5 + text_hi^0.5 + text_es^0.5  + hashtags^1.5 + mentions^1.2'

            if lang=='hi':
                fields = 'text_en^0.5 + text_hi^2.5 + hashtags^1.5 + mentions^1.2'

            if lang=='es':
                fields = 'text_en^1.0 + text_es^2.5 + hashtags^1.5 + mentions^1.2'

            if(detectedLang==lang):
                query=text
            else:
                query=convertToOtherLang(text,detectedLang,lang)
            result.extend(getQueryResult_BM25(fields, text)['response']['docs'])
        newlist = sorted(result, key=lambda d: d['score'],reverse=True)
        print(newlist)
        return newlist





@app.route('/getQueryResults',methods = ['GET'])
def process_query(query):
    result=getQueryResult_BM25(query)

if __name__ == '__main__':
    createQuery("vaccine")
    # app.run(debug=True,port='8000')
    # lang=detectLang("यह बहुत कठिन क्षण है")
    # text=convertToOtherLang("यह बहुत कठिन क्षण है",lang,'hi')



