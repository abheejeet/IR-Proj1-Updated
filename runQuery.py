# -*- coding: utf-8 -*-

import json
import requests
from flask import Flask
from googletrans import Translator
app = Flask(__name__)


AWS_PUBLIC_IP="18.117.83.76"
SOLR_CORE="IRF21P4001"


@app.route('/')
def hello():
    return 'Hello, World!'


URL="http://"+AWS_PUBLIC_IP+":8983/solr/"+SOLR_CORE+"/select"

def getQueryResult_BM25(fields,query):
    print(query)
    # fields='text_en^2.5 + text_ru^2.5 + text_de^2.5 + tweet_hashtags^1'
    PARAMS = {'defType':'dismax','qf':fields,'q':query,'fl':'id,score','wt':'json','indent':'true','rows':20}

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
    else:
        for lang in ['en','hi','es']:
            fields = 'text_' + lang + '^2.5 + hashtags^1.5 + mentions^1.2'
            if(detectedLang==lang):
                query=text
            else:
                query=convertToOtherLang(text,detectedLang,lang)
            result = getQueryResult_BM25(fields, text)
            print(result)





@app.route('/getQueryResults',methods = ['GET'])
def process_query(query):
    result=getQueryResult_BM25(query)

if __name__ == '__main__':
    # app.run(debug=True,port='8000')
    # lang=detectLang("यह बहुत कठिन क्षण है")
    # text=convertToOtherLang("यह बहुत कठिन क्षण है",lang,'hi')



