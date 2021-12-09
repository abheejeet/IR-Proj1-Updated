import pickle 
print(pickle.format_version)


filePath="crowdsourced_keywords.pickle"
with open(filePath, 'rb') as f:
    b = pickle.load(f)
    print(b)
