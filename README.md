# Building-ETL-pipeline-and-performing-analysis-on-Startup-Data


Use Case: -  The users wants to know the Insight form the Startup Data, so that they can take an Important decision which will be helpful from Business point of View. We perform some analysis on Startup data and done Visualization on top of it so that it can be easily understable by everyone.

Proposed Solution: - For building Data pipeline, we will be using various Google Cloud Platform services. 
The solution involves following GCP services :-
Google Cloud Storage :- Extracted raw data from external sources and loaded data in Google Cloud Storage.
Dataproc :- Created a Dataproc Cluster and developed Pyspark script to transform data using Dataproc. Created a Dataproc job to run the Pyspark script and the transformed data is store in Google Cloud Storage.
BigQuery :- Imported data in BigQuery from Google Cloud Storage.
Data Studio :- Connected Data Studio with BigQuery and done visualization in Data Studio.


