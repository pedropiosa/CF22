import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import random
import time

# Import the NLTK package
import nltk
import spacy

# Import the necessary modules (for NLP processing: stopwords and lemmatization)
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.probability import FreqDist


# Import the data
headers = ["Descr", "greenPlastic"]
df = pd.read_csv('dataExtended.csv', sep='\╬', header=None).apply(lambda x: x.str.replace(r"\"",""))
df.columns = headers

data = df.iloc[1:]

del df


# Remove stopwords and store text without stopwords in column 'WOstopwords' of data
counter = 0
# Define a function to extract the keywords from a text
def extract_keywords(text):
    global counter
    counter += 1
    start_time = time.time()
    # Tokenize the text into words
    words = word_tokenize(text)
    # # Convert the words to lowercase
    # words = [word.lower() for word in words]
    
    # # Remove punctuation symbols from the list of words
    # words = [word for word in words if word.isalnum()]
    
    # # Remove stopwords from the list of words
    # words = [word for word in words if word not in stopwords.words("english")]
      
    words = [word.lower() for word in words if word.isalnum() and word not in stopwords.words("english")]
    # words = list(filter(lambda x: x.isalnum(),map(lambda x: x.lower(),words)))
    # words = list(filter(lambda x: x not in stopwords.words("english"), words))



    # For preliminary analysys, calculate the frequency of each word in the list of words
    #fdist = FreqDist(words)
    
    # Store the text without stopwords
    textWithoutStopwords = ' '.join(words)
    
    print(time.time() - start_time,counter)
    
    # Return the list of keywords (the most frequent words in the text)
    #return fdist.most_common(max_number_keywords), textWithoutStopwords
    return textWithoutStopwords


# Define how may keywords we want to show
#max_number_keywords = 5


data['WOstopwords'] = data['Descr'].apply(extract_keywords)

# Lemmatize the text without stopwords and store it in column 'lemmzd'of data

#Load the Spacy language model
nlp = spacy.load("en_core_web_sm")

counter = 0

#Define a function to extract the keywords from a text
def lemmatize(text):
    global counter
    counter += 1
    start_time = time.time()
    
    #Process the text using the Spacy model
    doc = nlp(text)
  
    #Get the lemma for each word in the text
    lemmas = [token.lemma_ for token in doc]
  
    # For preliminary analysys, calculate the frequency of each word in the list of lemmas
    #fdist = FreqDist(lemmas)
    #Return the list of keywords (the most frequent lemmas in the text)
    #return fdist.most_common(max_number_keywords)
    
    lemmatizedText = ' '.join(lemmas)
    
    print(time.time() - start_time,counter)
    
    return lemmatizedText


# Lemmatize all the descriptions without stopwords
data['lemmzd'] = data['WOstopwords'].apply(lemmatize)


# Write the dataframe with a third column (text without stopwords)
data.to_csv("dataframeLemmatized.csv", sep='╬', index=False)

#%%
# Load the data including description without stopwords and lemmatization
headers = ["Descr", "greenPlastic", "WOstopwords", "lemmzd"]
df = pd.read_csv('dataframeLemmatized.csv', sep='\╬', header=None).apply(lambda x: x.str.replace(r"\"",""))
df.columns = headers
data =  df.iloc[1:]

del df

# Train the model only removing the stopwords
results = []

start_time = time.time()
random_number = random.randint(0, 100)
# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(data['WOstopwords'], data['greenPlastic'], test_size=0.2, random_state=random_number)

# Preprocessing
vectorizer = CountVectorizer()
X_train = vectorizer.fit_transform(X_train)
X_test = vectorizer.transform(X_test)

# Train the model
clf = LogisticRegression(max_iter=10000)

clf.fit(X_train, y_train)

# Predict on test set
y_pred = clf.predict(X_test)

accuracy = accuracy_score(y_test, y_pred)
precision = precision_score(y_test, y_pred, pos_label='1')
recall = recall_score(y_test, y_pred, pos_label='1')
f1 = f1_score(y_test, y_pred, pos_label='1')
results.append([accuracy, precision, recall, f1, random_number ,time.time() - start_time])

#Convert the list of results to numpy array
results = np.array(results)
    
# Evaluation
print("Accuracy mean: ", np.mean(results[:,0]))
print("Precision mean: ", np.mean(results[:,1]))
print("Recall mean: ", np.mean(results[:,2]))
print("F1 mean: ", np.mean(results[:,3]))
print("Execution time mean: ", np.mean(results[:,5]))

#%%
# Train the model removing the stopwords AND lemmatizing
results = []

for i in range(1):
    start_time = time.time()
    random_number = random.randint(0, 100)
    # Split the data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(data['lemmzd'], data['greenPlastic'], test_size=0.2, random_state=random_number)
    
    # Preprocessing
    vectorizer = CountVectorizer()
    X_train = vectorizer.fit_transform(X_train)
    X_test = vectorizer.transform(X_test)
    
    # Train the model
    clf = LogisticRegression(max_iter=10000)

    clf.fit(X_train, y_train)
    
    # Predict on test set
    y_pred = clf.predict(X_test)

    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred, pos_label='1')
    recall = recall_score(y_test, y_pred, pos_label='1')
    f1 = f1_score(y_test, y_pred, pos_label='1')
    results.append([accuracy, precision, recall, f1, random_number ,time.time() - start_time])
    
#Convert the list of results to numpy array
results = np.array(results)
   
# Evaluation
print("Accuracy mean: ", np.mean(results[:,0]))
print("Precision mean: ", np.mean(results[:,1]))
print("Recall mean: ", np.mean(results[:,2]))
print("F1 mean: ", np.mean(results[:,3]))
print("Execution time mean: ", np.mean(results[:,5]))
