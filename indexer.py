import os
import pysolr
import requests
import json
import pickle5 as pickle

# https://tecadmin.net/install-apache-solr-on-ubuntu/


CORE_NAME = "IRF21P4001"
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

    hindi = ['एंटीबॉडी']
    spanish = ['campaña de vacunación', 'efectos secundarios de la vacuna', 'vacuna covid']

    nonpois2 = ['vaccineshortage', 'covishield', 'booster shot', 'sinopharm', 'immunity', 'injection', 'herd immunity',
                'ivermectin', 'first dose', 'booster shots', 'firstdose', 'fully vaccinated', 'vaccineswork',
                'covidshield', 'fullyvaccinated', 'side effects', 'dose', 'novavax', 'j&j', 'covaxin', 'sputnikv']

    data = {
        'en': nonpois2,
        'hi': hindi,
        'es': spanish

    }

    data1={
        'en' : ['sinovac', 'astrazenca', 'vaccine jab', 'vaccine passport', 'vaccinepassport',
                'mrna vaccine', 'pfizer', 'vaccine efficacy', 'antibodies', 'getvaccinated', 'booster',
                'largestvaccinedrive', 'vaccine hesitancy', 'cowin', 'vaccinate', 'clinical trials',
                'johnson and johnson', 'vaccinationdrive', 'clinical trial', 'vaccinemandate', 'vaccine side effects',
                'covid-19 vaccine', 'largestvaccinationdrive', 'doses', 'remdesivir', 'covid19vaccine', 'vaccinequity','vaccinesideeffects', 'vaccinated', 'vaccinessavelives', 'jab', 'get vaccinated', 'side effect',
               'covaxine', 'mrna', 'getvaccinatednow', 'vaccinepassports',  'vaccinehesitancy',
               'sputnik', 'johnson & johnson’s janssen', 'unvaccinated', 'janssen', 'sputnik v', 'astra zeneca',
               'getvaxxed', 'johnson', 'vaccine passports', 'vaccination drive','vaccine dose', 'we4vaccine','johnson & johnson', 'vaccine mandate', 'covidvaccine', 'zycov-d', 'vaccines', '#largestvaccinedrive'],
        'hi':['टीके', 'वैक्सीनेशन', 'वैक्सीन पासपोर्ट', 'दूसरी खुराक', 'वैक्सीन के साइड इफेक्ट', 'टीका', 'वैक्सीन जनादेश', 'कोवेक्सिन', 'कोविड का टीका', 'कोवैक्सिन', 'फाइजर', 'कोवैक्सीन', 'वैक्सीन', 'प्रभाव', 'वैक्सीन', 'दुष्प्रभाव', 'टीका लगवाएं', 'एमआरएनए वैक्सीन', 'एस्ट्राजेनेका', 'कोविड टीका', 'लसीकरण', 'टीका_जीत_का', 'sabkovaccinemuftvaccine', 'कोविन', 'vaccinesamvaad', 'vaccinesamvad', 'खराब असर', 'tikautsav', 'रोग प्रतिरोधक शक्ति', 'tikakaran', 'कोविशिल्ड', 'खुराकवाइरस', 'teeka', 'पूर्ण टीकाकरण', 'पहली खुराक', 'टीकाकरण', 'टीकाकरण अभियान'],
        'es':['dosis de vacuna','inyección de refuerzo', 'inmunización', 'vacunado', 'efecto secundario', 'yomevacunoseguro', 'cdc', 'estrategiadevacunación', 'seconddose', 'vacunación', 'efectos secundarios', 'eficacia', 'anticuerpo', 'pasaporte de vacuna', 'la inmunidad de grupo', 'second dose', 'vacuna para el covid-19', 'completamente vacunado', 'vacúnate', 'vacunada', 'inmunidad', 'mandato de vacuna', 'vacunacion', 'primera dosis', 'segunda dosis', 'vacuna', 'pinchazo', 'dosis', 'vacunaton', 'vacunarse', 'eficacia de la vacuna', 'vacunar', 'vacunacovid19','cansino', 'vacunas']
    }

    pois = ["PMOIndia", "BernieSanders","JoeBiden","GavinNewsom","KamalaHarris","CDCgov","myogiadityanath","MoHFW_INDIA","AmitShah","ShashiTharoor","lopezobrador_", "alfredodelmazo", "COFEPRIS", "Claudiashein","SSalud_mx"]
    poisC = ["PMOIndia", "BernieSanders", "JoeBiden"]
    for poi in pois:
        fileName = "./data/POI_" + poi + ".pickle"
        with open(fileName, 'rb') as f:
            docs = pickle.load(f)
        # print(docs)
        i.create_documents(docs)
    nonpois=['vaccines']
    nonpois2= ['vacuna covid', 'campaña de vacunación']

    dummy=['narendramodi','AmitShah']


    for item in poisC:
        fileName = "./data/POI_Replies" + item + ".pickle"
        with open(fileName, 'rb') as f:
            docs = pickle.load(f)
        # print(docs)
        i.create_documents(docs)

    for item in nonpois:
        fileName = "./data/Non_POI_" + item + ".pickle"
        with open(fileName, 'rb') as f:
            docs = pickle.load(f)
        # print(docs)
        i.create_documents(docs)


    for item in nonpois:
        fileName = "./data/Non_POI_Replies" + item + ".pickle"
        with open(fileName, 'rb') as f:
            docs = pickle.load(f)
        # print(docs)
        i.create_documents(docs)

    for lang in ['en', 'hi', 'es']:
        for tweet in data.get(lang):
            try:
                fileName = "NON_POI/" + lang + "/" + tweet + "_NON_REPLIES.pickle"
                with open(fileName, 'rb') as f:
                    docs = pickle.load(f)
                i.create_documents(docs)
            except Exception as e:
                if 'atomic' in str(e):
                    print(" problem file : " + fileName)
                    continue
            # print(docs)
            # i.create_documents(docs)

        for tweet in data.get(lang):
            try:
                fileName = "NON_POI/" + lang + "/" + tweet + ".pickle"
                with open(fileName, 'rb') as f:
                    docs = pickle.load(f)
                i.create_documents(docs)
            except Exception as e:
                if 'atomic' in str(e):
                    print(" problem file : " + fileName)
                    continue

            # print(docs)
            # i.create_documents(docs)

        for tweet in data.get(lang):
            try:
                fileName = "NON_POI/" + lang + "/" + tweet + "_REPLIES.pickle"
                with open(fileName, 'rb') as f:
                    docs = pickle.load(f)
                i.create_documents(docs)
            except Exception as e:
                if 'atomic' in str(e):
                    print(" problem file : "+fileName)
                    continue

    for lang in ['en', 'hi', 'es']:
        # for tweet in data1.get(lang):
            # try:
            #     fileName = "NON_POI/" + lang + "/" + tweet + "_NON_REPLIES.pickle"
            #     with open(fileName, 'rb') as f:
            #         docs = pickle.load(f)
            #     i.create_documents(docs)
            # except Exception as e:
            #     if 'atomic' in str(e):
            #         print(" problem file : " + fileName)
            #         continue
            # print(docs)
            # i.create_documents(docs)

        for tweet in data1.get(lang):
            try:
                fileName = "NON_POI/" + lang + "/" + tweet + ".pickle"
                with open(fileName, 'rb') as f:
                    docs = pickle.load(f)
                i.create_documents(docs)
            except Exception as e:
                if 'atomic' in str(e):
                    print(" problem file : " + fileName)
                    continue

            # print(docs)
            # i.create_documents(docs)

        # for tweet in data.get(lang):
        #     try:
        #         fileName = "NON_POI/" + lang + "/" + tweet + "_REPLIES.pickle"
        #         with open(fileName, 'rb') as f:
        #             docs = pickle.load(f)
        #         i.create_documents(docs)
        #     except Exception as e:
        #         if 'atomic' in str(e):
        #             print(" problem file : " + fileName)
        #             continue

        for item in dummy:
            fileName = "DummyReplies/"+item+".pickle"
            with open(fileName, 'rb') as f:
                docs = pickle.load(f)
            # print(docs)
            i.create_documents(docs)

