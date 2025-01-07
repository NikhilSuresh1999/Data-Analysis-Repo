import pandas as pd
import pyodbc
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

#Download the Vader lexicon for sentiment analysis if not already present
nltk.download('vader_lexicon')


#define function to fetch data from sql database using sql query
def fetch_data_from_sql():
  #Define connection String with the parameters for database connection
  conn_str = (
    "Driver={SQL Server};"
    "Server=LAPTOP-SGV1EDUG\SQLEXPRESS;"
    "Database=PortfolioProject_MarketingAnalytics;"
    "Trusted_Connection=yes;"
  )
  
  #Establish the connection to the database
  conn = pyodbc.connect(conn_str)
  
  #Define the sql query to fetch customer review data
  query = "Select ReviewID,CustomerID,ProductID,ReviewDate,Rating,ReviewText from customer_reviews"
  
  #Execute the query and fetch data into data frame
  df = pd.read_sql(query,conn)
  
  conn.close()
  
  return df

#Fetch customer reviews data from sql database
customer_reviews_df = fetch_data_from_sql()

#Initialize the vader sentiment intensity analyzer for analysing the sentiment of text data
sia = SentimentIntensityAnalyzer()


#Define a function to calculate Sentiment score using Vader
def calculate_sentiment(review):
  #get the sentiment score for the review text
  sentiment = sia.polarity_scores(review)
  #Return the compound score which is normalized score between most negative -1 and most positive 1
  return sentiment['compound']


#define a function to categorize sentiment using both sentiment score and review ratings
def categorize_sentiment(score,rating):
  #use both text sentiment score and numeric rating to determine sentiment category
  
  if score > 0.05:
    if rating >= 4:
      return 'Positive'
    
    elif rating == 3:
      return 'Mixed Positive'
    
    else :
      return 'Mixed Negative'
    
  elif score < -0.05:
    if rating <= 2:
      return 'Negative'
    
    elif rating == 3:
      return 'Mixed Negative'
    
    else:
      return 'Mixed Positive'
  
  else:
    if rating >=4:
      return 'Positive'
    
    elif rating <= 2:
      return 'Negative'
    
    else:
      return 'Neutral'
    


#Define function to bucket sentiment score to bucket ranges

def sentiment_bucket(score):
  if score >= 0.5:
    return '0.5 to 1.0'  #strongly positive sentiment 
  
  elif 0.0 <= score < 0.5:
    return '0.0 to 0.49' #midly positive sentiment
  
  elif -0.5 <= score < 0.0:
    return '-0.49 to 0.0'   #midly negative sentiment
  
  else:
    return '-1.0 to -0.5'   #strongly negative sentiment
  



#Apply sentiment  analysis to calculate sentiment score for each review

customer_reviews_df['SentimentScore'] = customer_reviews_df['ReviewText'].apply(calculate_sentiment)

#Apply sentiment catogarization on both text and rating
customer_reviews_df['SentimentCategory'] = customer_reviews_df.apply(
  lambda row: categorize_sentiment(row['SentimentScore'],row['Rating']),axis = 1)


#Apply sentiment bucketing to categorize scores into defined ranges
customer_reviews_df['SentimentBucket'] = customer_reviews_df['SentimentScore'].apply(sentiment_bucket)


#Display first few rows of data frame with sentiment score , categories and bucket
print(customer_reviews_df.head())

#Save the dataframe values as a csv formated file

customer_reviews_df.to_csv('fact_customer_review_with_sentiments.csv',index = False)
      


  
  