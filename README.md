## Recommender System
We have built a basic recommender system based on an open source recommend framework called PredictionIO, which consists of three parts: the Event server receives events about user behaviors, such as adding new user, adding new item, and the actions the user made to items, and then events will store these behavior data, and another part recommendation engine can read these data, and training a recommendation model, and the last one is a web service, which accept user request like recommending 10 items to a special users, and then the web service will all recommendation model service and response the recommendation results.  


### Environment Setup
- Scala
- Spark
- PredictionIO: http://predictionio.apache.org/install/

### Load Data
- create app
- cd data folder
- run command `python3 import_movie_data.py --access_key=$access_key`

### Todo List

- Implement evaluation
- basic algorithm
    - implement the content-based of calculating user similarity base user profile
    - implement the content-based of calculating profile similarity base movie profile
    - implement user-based collaborative filtering algorithm
    - implement item-based collaborative filtering algorithm
    - implement association rules(FPGrowth)
- Hybrid filtering
    - weighting
    - filtering
- recommender result sorting
    - GBDT + LR
- Online learning
    - FTRL

