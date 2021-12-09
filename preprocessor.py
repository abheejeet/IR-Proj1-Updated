import os
import pickle
import pandas as pd
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.sentiment import SentimentAnalyzer
import nltk
nltk.download('punkt')
nltk.download('vader_lexicon')
from nltk import tokenize
from googletrans import Translator
from textblob import TextBlob
import re

path ="./Sample/"
#we shall store all the file names in this list
filelist = []


def remove_URL(sample):
    """Remove URLs from a sample string"""
    return re.sub(r"http\S+", "", sample)

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

def isCovidTweet(sentence):
    filePath = "crowdsourced_keywords.pickle"
    covid=[]
    with open(filePath, 'rb') as f:
        b = pickle.load(f)
        covid=b['covid']

    tWords = sentence.split()

    for word in covid:
        if word in tWords:
            return True


    return False

def isVaccineTweet(sentence):
    filePath = "crowdsourced_keywords.pickle"
    covid = []
    with open(filePath, 'rb') as f:
        b = pickle.load(f)
        covid = b['vaccine']

    tWords = sentence.split()

    for word in covid:
        if word in tWords:
            return True

    return False


def sentimentAnalysis(sentence):
    sentence=remove_URL(sentence)
    if(len(sentence)==0):
        return {'neg': 0.0, 'neu': 0.0, 'pos': 0.0, 'compound': 0.0}
    detectedLang = detectLang(sentence)
    if detectedLang != 'en':
        sentence =convertToOtherLang(sentence,detectedLang,'en')

    sid = SentimentIntensityAnalyzer()
    ss = sid.polarity_scores(sentence)
    return ss,sentence



def processPickleFiles(file,name,count):
    name=os.path.splitext(name)[0]
    try:
        with open(file, 'rb') as f:
            docs = pickle.load(f)
            for doc in docs:
                try:
                    sentiment,sentence = sentimentAnalysis(doc.get('tweet_text'))
                    doc['negative_sentiment'] = sentiment.get('neg')
                    doc['positive_sentiment'] = sentiment.get('pos')
                    doc['neutral_sentiment'] = sentiment.get('neu')
                    doc['compound_sentiment'] = sentiment.get('compound')
                    doc['translated_to_english']=sentence
                    doc['is_covid_tweet']=isCovidTweet(doc.get('tweet_text')) or isCovidTweet(sentence)
                    doc['is_vaccine_tweet']=isVaccineTweet(doc.get('tweet_text')) or isVaccineTweet(sentence)
                except Exception as e:
                    print(f"Failed for tweet in File {file} : {doc.get('id')} " + str(e))

            with open(f"ProcessedData/{name}_{count}.pickle", 'wb') as sb:
                pickle.dump(docs, sb, protocol=pickle.HIGHEST_PROTOCOL)

                # print(sentiment)
            # print(docs[0])
    except Exception as e:
        print(f"Failed for File {file} "+ str(e))


def processPklFiles(file,name,count):
    name=os.path.splitext(name)[0]
    # print(file)
    try:
        df = pd.read_pickle(file)
        negative=[]
        positive=[]
        neutral=[]
        compound=[]
        translated_to_english=[]
        covid_tweets=[]
        vaccine_tweets=[]
        for index, row in df.iterrows():
            try:
                sentiment,sentence=sentimentAnalysis(row['tweet_text'])
                negative.append(sentiment.get('neg'))
                positive.append(sentiment.get('pos'))
                neutral.append(sentiment.get('neu'))
                compound.append(sentiment.get('compound'))
                translated_to_english.append(sentence)
                covid_tweets.append(isCovidTweet(row['tweet_text']) or isCovidTweet(sentence))
                vaccine_tweets.append(isVaccineTweet(row['tweet_text']) or isCovidTweet(sentence))
            except Exception as e:
                print(f"Failed for tweet in File {file} : {row['id']} " + str(e))

        df['negative_sentiment']=negative
        df['positive_sentiment'] = positive
        df['neutral_sentiment'] = neutral
        df['compound_sentiment']= compound
        df['translated_to_english']=translated_to_english
        df['is_covid_tweet']=covid_tweets
        df['is_vaccine_tweet']=vaccine_tweets
        df.to_pickle(f"ProcessedData/{name}_{count}.pkl")
    except Exception as e:
        print(f"Failed for File {file} "+ str(e))



def read_Files():
    count=0
    for root, dirs, files in os.walk(path):
        for file in files:
            if(file.endswith(".pickle")):
                print(os.path.join(root, file))
                processPickleFiles(os.path.join(root, file),file,count)
                count+=1

            if(file.endswith(".pkl")):
                print(os.path.join(root, file))
                processPklFiles(os.path.join(root, file),file,count)
                count+=1



if __name__ == '__main__':


    read_Files()
    # # processPklFiles("./Data/Aditya/keywords_covishield.pkl","keywords_covishield.pkl",1)
    # # sentimentAnalysis("This is a sentiment")
    # processPickleFiles("./Data/Abheejeet/Data/POI_AmitShah.pickle","sdasa.txt",1)
    # print(isVaccineTweet("vaccineshortage"))
    # print(isCovidTweet("vaccineshortage"))
    # print(isCovidTweet("मुखौटा जनादेश"))
    # read_Files()