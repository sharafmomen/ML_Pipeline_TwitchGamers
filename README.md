<p align="center">
  <img src="https://github.com/sharafmomen/ML_Pipeline_TwitchGamers/blob/main/images/ETL_Pipeline_for_Pro_Players_on_League_of_Legends_%F0%9F%8E%AE_.png" width="1000">
</p>

# LeagueOfLegends_Pipeline
This project aims to make a Data Pipeline, which tracks all the top streamers (or streamers currently online), and then uses their information to extract the features relating to the 1st 10 minutes of their last game to predict who will win. The scheduled automation and scripting process is all done using Airflow. Here is a quick recap of the architecture, data sourcing and storage, and specifics of the ML pipeline. 

## Architecture
The tools we use in this project are:
1. Apache Airflow
2. PySpark
3. AWS RDS and PostgreSQL
4. Flask
5. Faculty AI

In this project, we have approximately 5 python scripts, of which one holds the DAG that Airflow can read. 3 other python scripts host the functions necessary for ETL, which the DAG script imports and makes use of. Only port 8888 can be used to host an app, which is Airflow. Jupyter must be closed, and this is what Faculty allows. The following represents a snippet of our code defining our ETL DAG and some functions encased in a Python Operator:

```python
with DAG('ETL_streamers_eSports', default_args={'retries': 0}, description="ETL for streamers' games", schedule_interval='@hourly', 
  start_date=pendulum.datetime(2022, 4, 20, tz="UTC"), catchup=False, tags=['streamers_lol_etl']) as dag:
    
    t1 = PythonOperator(
        task_id='Extract',
        python_callable=extract,
    )
    
    t2 = PythonOperator(
        task_id='Transform',
        python_callable=transform,
    )
    
    t3 = PythonOperator(
        task_id='Load',
        python_callable=load,
    )

    t4 = PythonOperator(
        task_id='ML_Pipeline_Half',
        python_callable=ml_training,
    )

    t1 >> t2 >> t3 >> t4
```
AWS RDS is a service that helps to manage and store data into PostgreSQL, a database management system. A combination of both help to ensure data integrity through PostgreSQL’s A.C.I.D compliance (atomicity, consistency, isolation, and durability), especially as we consider the data that we collect OLTP. Pyspark was primarily used for processing data, and was chosen for created a distributed pipeline and scalability for when large amounts of data is ingested. Faculty AI is a cloud service that is a docker environment built on top of AWS EC2 instances. Flask is a tool that enables users to create web applications easily through Python. Faculty AI provides us an option to easily deploy and serve models through a Flask application through a REST API. 

## Data Sourcing and Storage
Given that Riot Games has given access to their data through an API, we will make use of RiotWatcher to extract the 1st 10 minutes of games from all popular streamers from a particular website – Tracking the Pros. Within processes between Extract and Tranform in our ETL processes, we use temporary CSV files to send data to the next task on the Airflow DAG. Ultimately, the data is stored in these schema:
<p align="center">
  <img src="https://github.com/sharafmomen/ML_Pipeline_TwitchGamers/blob/main/images/schema.png" width="900">
</p>

## ETL Pipeline

The following Airflow diagram captures the ETL portion of the project. 
<p align="center">
  <img src="https://github.com/sharafmomen/ML_Pipeline_TwitchGamers/blob/main/images/ETL.png" width="500">
</p>
While collecting data on streamers from Track The Pros and finding their gameplay information through the Riot API, we first look for the number of cores on the server hosting the application. Then we divide the load onto multiple processors using multiprocessing in order to quicken the pace. Asynchronous programming is another alternative. 

Transformations were essential, as most of the data we were getting were time-series based, meaning each row represented an event or status of the player at a particular time. Then a grouping function is formed to aggregate all the kills and other events like warding, to be player-focused - each row would represent one player's summary. Anything above 10 minutes was removed, prior to this aggregation. 

Psycopg2 was used to load the transformations into Postgres. It first made sure to see if the tables existed. If it didn't, then it would create the tables prior to loading it onto the database:
<p align="center">
  <img src="https://github.com/sharafmomen/ML_Pipeline_TwitchGamers/blob/main/images/create_tables.png" width="600">
</p>

## ML Pipeline

<p align="center">
  <img src="https://github.com/sharafmomen/ML_Pipeline_TwitchGamers/blob/main/images/ml_pipeline.png" width="600">
</p>
The above DAG graph captures the extension on top of the ETL pipeline made. With each DAG run, the model will update, however, we can see the performance prior to approving the new model's deployment - this last portion of the process is made manual for contingency purposes. We considered using PySpark's MLlib library to do the distributed training, should we want to scale the data we ingest. The issue is that it is difficult to serve such models without proper Java implementation, and without starting up a spark instance to turn the request into a Spark Data Frame before predicting on it. Hence, we move on with Scikit-learn. 

The deployment process is done separately from the entire ETL and half of the ML pipeline. Faculty AI makes it easy for you, as their deployment process is specifically made for flask applications. The Faculty Deployment interface also provides you the chance to play with the API and the flask app before deployment on a testing terminal. Lastly, after deployment, we test the API by using a curl bash command:
<p align="center">
  <img src="https://github.com/sharafmomen/ML_Pipeline_TwitchGamers/blob/main/images/curl_request.png" width="500">
</p>

It's a success! However, I did take note of some improvements we could make. 
1. Celery executor can help with simultaneously getting tasks of an Airflow DAG done. 
2. Using a Developer's API key instead of a personal one to bypass Riot API's very limiting rate limits. 
3. Making use of MLeap to deploying PySpark Models, which would help with the distributed process more to cater to future scalability. 
4. Making using of neptune.ai and MLflow to automate detecting data drift. 
5. Using aynchronous functions rather than multiprocessing, as most of the early load of the ETL pipeline is waiting for a request to finish processing. 
