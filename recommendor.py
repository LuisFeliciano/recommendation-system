# Recommendor System for E-Commerce 
# 
# CPS 491 Capstone II Team 4 University of Dayton
# Developed by Luis Feliciano
# 
from lenskit.algorithms import Recommender
from lenskit.algorithms.user_knn import UserUser
import pandas as pd
import pymongo
import constants
import time
import schedule

def updateMostPurchased(purchaseData, mostPurchasedCollection):

  mostPurchasedCollection.delete_many({})

  mostPurchased = purchaseData['productID'].value_counts().to_dict()

  for key in mostPurchased:
    entry = {"productID": key, "purchaseAmount": mostPurchased[key]}
    mostPurchasedCollection.insert_one(entry)

def updateHighestRated(purchaseData, highestRatedCollection):

  highestRatedCollection.delete_many({})

  average_ratings = (purchaseData).groupby('productID').agg(count=('username', 'size'), rating=('rating', 'mean')).reset_index()

  sorted_avg_ratings = average_ratings.sort_values(by="rating", ascending=False)

  highestRated = sorted_avg_ratings.to_dict('records')

  for entry in highestRated:
    highestRatedCollection.insert_one(entry)

def updateUserRecommendations(userRatings, purchaseData, userRecommendationsCollection):

  userRecommendationsCollection.delete_many({})

  purchaseData.columns = ['_id', 'item', 'user', 'quantity', 'rating', 'timestamp']
  numberOfRecs = 10
  algorithmUserUser = UserUser(15, min_nbrs=3)
  algo = Recommender.adapt(algorithmUserUser)
  algo.fit(purchaseData)

  for item in userRatings:
    userRate = item["ratings"]
    userRates = {}

    for k, v in userRate.items():
      userRates[int(k)] = v

    userRecommendation = algo.recommend(-1, numberOfRecs, ratings=pd.Series(userRates))

    recommendations = []

    productIDList = userRecommendation['item'].tolist()
    ratingList = userRecommendation['score'].tolist()

    for i in range(numberOfRecs):
      recommendation = {"productID": productIDList[i], "score:": ratingList[i]}
      recommendations.append(recommendation)

    entry = {"usernname": item["username"], "recommendations": recommendations}

    userRecommendationsCollection.insert_one(entry)

def pullPurchaseData(purchaseCollection):
  purchaseList = []

  for element in purchaseCollection.find():
    purchaseList.append(element)

  purchaseDataFrame = pd.DataFrame(purchaseList)

  return purchaseDataFrame

def pullUserRatings(userRatingsCollection):
  userRatings = []

  for element in userRatingsCollection.find():
    userRatings.append(element)
  
  return userRatings

def databaseconnection():
  client = pymongo.MongoClient(constants.CONNECTION_LINK)

  database = client["Store"]

  return database

def job(db):
  # Pull purchase data from purchases collection
  purchaseData = pullPurchaseData(db["purchases"])

  # Pull user ratings data from userRatings collection
  userRatings = pullUserRatings(db["userRatings"])

  # Update mostPurchased and highestPurchased collection using purchase data
  updateMostPurchased(purchaseData, db["mostPurchased"])
  updateHighestRated(purchaseData, db["highestRated"])

  # Update userRecommendations collectin using puchase and user rating data
  updateUserRecommendations(userRatings, purchaseData, db["userRecommendations"])

  print("Recommendation System has updated userRecommendations, mostPurchased, and highestRated collections")

def main():

  print("CPS 491 Capstone Team 4 Recommendation System")

  # Establish connection with MongoDB
  db = databaseconnection()

  # Setting the time for when the Recommendation System will perform recommendations
  schedule.every().hour.do(job, db)

  while True:
    schedule.run_pending()
    time.sleep(30)

if __name__ == "__main__":
  main()