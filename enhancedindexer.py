import os
import pysolr
import requests
import json
import pickle
import pandas as pd

path ="./ProcessedData/"

# https://tecadmin.net/install-apache-solr-on-ubuntu/


CORE_NAME = "IRF21P4015"
AWS_IP = "localhost"


# [CAUTION] :: Run this script once, i.e. during core creation


def delete_core(core=CORE_NAME):
    print(os.system('sudo su - solr -c "/opt/solr/bin/solr delete -c {core}"'.format(core=core)))


def create_core(core=CORE_NAME):
    print(os.system(
        'sudo su - solr -c "/opt/solr/bin/solr create -c {core} -n data_driven_schema_configs"'.format(
            core=core)))


class Indexer:
    def __init__(self):
        self.solr_url = f'http://{AWS_IP}:8983/solr/'
        self.connection = pysolr.Solr(self.solr_url + CORE_NAME, always_commit=True, timeout=5000000)

    def do_initial_setup(self):
        delete_core()
        create_core()

    def create_documents(self, docs):
        print(self.connection.add(docs))

    def add_fields(self):
        data = {
            "add-field": [
                {
                    "name": "poi_name",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "poi_id",
                    "type": "plong",
                    "multiValued": False
                },
                {
                    "name": "verified",
                    "type": "boolean",
                    "multiValued": False
                },
                {
                    "name": "country",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "replied_to_user_id",
                    "type": "plong",
                    "multiValued": False

                },
                # {
                #    "name": "id",
                #    "type": "string",
                #    "multiValued": False
                # },
                {
                    "name": "replied_to_tweet_id",
                    "type": "plong",
                    "multiValued": False
                },
                {
                    "name": "reply_text",
                    "type": "text_general",
                    "multiValued": False
                },
                {
                    "name": "tweet_text",
                    "type": "text_general",
                    "multiValued": False
                },
                {
                    "name": "tweet_lang",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "text_hi",
                    "type": "text_hi",
                    "multiValued": False
                },
                {
                    "name": "text_en",
                    "type": "text_en",
                    "multiValued": False
                },
                {
                    "name": "text_es",
                    "type": "text_es",
                    "multiValued": False
                },
                {
                    "name": "hashtags",
                    "type": "string",
                    "multiValued": True
                },
                {
                    "name": "mentions",
                    "type": "string",
                    "multiValued": True
                },
                {
                    "name": "tweet_urls",
                    "type": "string",
                    "multiValued": True
                },
                {
                    "name": "tweet_emoticons",
                    "type": "string",
                    "multiValued": True
                },
                {
                    "name": "tweet_date",
                    "type": "pdate",
                    "multiValued": False
                },
                {
                    "name": "geolocation",
                    "type": "string",
                    "multiValued": True
                },
                {
                    "name": "negative_sentiment",
                    "type": "pfloat",
                    "multiValued": False
                },
                {
                    "name": "positive_sentiment",
                    "type": "pfloat",
                    "multiValued": False
                },
                {
                    "name": "neutral_sentiment",
                    "type": "pfloat",
                    "multiValued": False
                },
                {
                    "name": "compound_sentiment",
                    "type": "pfloat",
                    "multiValued": False
                },
                {
                    "name": "translated_to_english",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "is_vaccine_tweet",
                    "type": "boolean",
                    "multiValued": False
                },
                {
                    "name": "is_covid_tweet",
                    "type": "boolean",
                    "multiValued": False
                },
            ]
        }

        print(requests.post(self.solr_url + CORE_NAME + "/schema", json=data).json())


    def replace_BM25(self, b=None, k1=None):
        data = {
            "replace-field-type": [
                {
                    'name': 'text_en',
                    'class': 'solr.TextField',
                    'positionIncrementGap': '50',
                    'indexAnalyzer': {
                        'tokenizer': {
                            'class': 'solr.StandardTokenizerFactory'
                        },
                        'filters': [
                            {
                            'class': 'solr.LowerCaseFilterFactory'
                        }, {
                            'class': 'solr.EnglishPossessiveFilterFactory'
                        }, {
                            'class': 'solr.KeywordMarkerFilterFactory',
                            'protected': 'protwords.txt'
                        }, {
                            'class': 'solr.PorterStemFilterFactory'
                        },{ 'class':'solr.CommonGramsFilterFactory' , 'words':'stopwords.txt','ignoreCase': 'true'  }

                        ]
                    },
                    'similarity': {
                        'class': 'solr.BM25SimilarityFactory',
                        'b': str(b),
                        'k1': str(k1)
                    },
                    'queryAnalyzer': {
                        'tokenizer': {
                            'class': 'solr.StandardTokenizerFactory'
                        },
                        'filters': [{
                            'class': 'solr.SynonymGraphFilterFactory',
                            'expand': 'true',
                            'ignoreCase': 'true',
                            'synonyms': 'synonyms.txt'
                        }, {
                            'class': 'solr.StopFilterFactory',
                            'words': 'lang/stopwords_en.txt',
                            'ignoreCase': 'true'
                        }, {
                            'class': 'solr.LowerCaseFilterFactory'
                        }, {
                            'class': 'solr.EnglishPossessiveFilterFactory'
                        }, {
                            'class': 'solr.KeywordMarkerFilterFactory',
                            'protected': 'protwords.txt'
                        }, {
                            'class': 'solr.PorterStemFilterFactory'
                        }]
                    }
                },{
                    'name': 'text_hi',
                    'class': 'solr.TextField',
                    'positionIncrementGap': '50',
                    'indexAnalyzer': {
                        'tokenizer': {
                            'class': 'solr.StandardTokenizerFactory'
                        },
                        'filters': [
                            {
                            'class': 'solr.IndicNormalizationFilterFactory'
                        }, {
                            'class': 'solr.HindiNormalizationFilterFactory'
                        }, {
                            'class': 'solr.HindiStemFilterFactory'
                        }
                        ]
                    },
                    'similarity': {
                        'class': 'solr.BM25SimilarityFactory',
                        'b': str(b),
                        'k1': str(k1)
                    },
                    'queryAnalyzer': {
                        'tokenizer': {
                            'class': 'solr.StandardTokenizerFactory'
                        },
                        'filters': [
                            {
                                'class': 'solr.IndicNormalizationFilterFactory'
                            }, {
                                'class': 'solr.HindiNormalizationFilterFactory'
                            }, {
                                'class': 'solr.HindiStemFilterFactory'
                            }
                        ]
                    }
                }, {
                    'name': 'text_es',
                    'class': 'solr.TextField',
                    'positionIncrementGap': '50',
                    'indexAnalyzer': {
                        'tokenizer': {
                            'class': 'solr.StandardTokenizerFactory'
                        },
                        'filters': [
                            {
                            'class': 'solr.LowerCaseFilterFactory'
                        }, {
                            'class': 'solr.SpanishLightStemFilterFactory'
                        }
                        ]
                    },
                    'similarity': {
                        'class': 'solr.BM25SimilarityFactory',
                        'b': str(b),
                        'k1': str(k1)
                    },
                    'queryAnalyzer': {
                        'tokenizer': {
                            'class': 'solr.StandardTokenizerFactory'
                        },
                        'filters': [
                            {
                                'class': 'solr.LowerCaseFilterFactory'
                            }, {
                                'class': 'solr.SpanishLightStemFilterFactory'
                            }
                        ]
                    }
                }
            ]
        }

        print(requests.post(self.solr_url + CORE_NAME + "/schema", json=data).json())


if __name__ == "__main__":
    i = Indexer()
    i.do_initial_setup()
    i.replace_BM25(b=0.8, k1=0.2)
    i.add_fields()

    A_count = 0
    Ad_count = 0
    for root, dirs, files in os.walk(path):
        for file in files:
            fileName = os.path.join(root, file)
            if (file.endswith(".pickle")):
                try:
                    with open(fileName, 'rb') as f:
                        docs = pickle.load(f)
                    i.create_documents(docs)
                    A_count += 1
                except Exception as e:
                    print("Some Exception in Abheejeet : " + str(e))
                    if 'atomic' in str(e):
                        print(" problem file : " + fileName)
                        continue

            if (file.endswith(".pkl")):
                print(fileName)
                df = pd.read_pickle(fileName)
                i.create_documents(df.to_dict('records'))
                Ad_count += 1

    print("Total Abheejeet Read Files: " + str(A_count))
    print("Total Aditya Read Files: " + str(Ad_count))

