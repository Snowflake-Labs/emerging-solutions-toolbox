# Overview

This tool is an adaptation of a subset of features from an internal fully-native Snowflake application called 'SnowPatrol'. 

Snowflake's CIO organization actively uses SnowPatrol to manage costs related to various in-house SaaS applications across departments. This is saving actual $$$ annually. Hear what Snowflake's CIO has to say about it : [Solution to optimize Software License ](https://www.youtube.com/watch?v=ys-zI5cRs6c)

While the full arc of SnowPatrol in-production within Snowflake IT covers many capabilities like auto-provisioning of most popular applications based on persona (learned through Snowpark ML model), understanding license usage to consider for revocation  and re-allocation (also learnt through Snowpark ML models), this tool provides one capability from this set, that of license revocation recommendations based on active usage.

The entire pipeline is fully native to Snowflake ans uses cutting edge features like **Snowpark for AI/ML**, **Streamlit (OSS)**, **Schema Evolution for CSVs** and others.


# Revocation Recommendations

License usage is learnt from the 1st party data - user authentication logs from sources such as Okta and/or directly from SaaS apps. 

A ML model (Logistic Regression) is trained on this data alongwith metadata like employee department, division, title attributes, work day schedule (to take into account working days and holidays for actual usage calculation). <br/>

The model is then used to predict the probability of near-future logins which becomes the basis of recommending license revocations. 

## Model training

The basis of understanding the login pattern of users are the authentication logs captured from various sources. Within Snowflake this comes from the tracked applications directly as well as Okta for a subset of them. Therefore this MVP also uses the same simulated data.

The way the combined authentication logs from both the sources are split to extract predictor and predicted features can be summed up as:

insert_img(feature_engineering.png, Feature Engineering from application's authentication logs)

This Streamlit app will allow the user to adjust the **_Cutoff date_** and the **_Probabilty threshold_** before training a new model over the data. 

- **_Cutoff date_** is always in comparison to the current date when the training is being run. This dates help to split the source datasets into two logical groups - (1) The logins before cutoff which are used to calculate predictor variables such as _weighted_authentication_ etc., (2) The logins after cutoff which are used to calculate the predicted variable such as _did_not_login_. 
_did_not_login_ is an indicator whether the user made a repeat login to the app with a period of 30days after the supposed _cutoff date_. 
:red[NOTE!] - the sample data supplied with this app has the maximum login date of June/8 and therefore the **_cutoff date_** has to be set for more than 45days atleast in order to select a point-of-reference within logs that gives enough 'logins after' to create the predicted variables.   

- **_Probability threshold_** is between 0 - 1 and represents an appetite for tolerance of the predicted probability of a repeat login based on the pattern learnt through training. A higher threshold would result in more revocation recommendation numbers, while a lower threshold would mean less number of such recommendations.

## Model runs

The revocation models are trained one at a time for each application which can be selected from a drop-down. 

This app provides two options to get the recommendations:

- Pull data from a previous run. For this you will need to select a 'run-id' of any previous execution for that app.

- Generate fresh recommendations on that day by re-training the model on the data from sources (pre-configured through a pipeline beforehand).



And that's all folks! Please navigate to the next page in this app by selecting the 'Getting Started' from the left bar. 