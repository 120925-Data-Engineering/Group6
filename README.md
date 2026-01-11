# Group6
Raziya Ahmed and Subre Moktar

Hello to our mock stream analytics platform.
Everything we created needs to be run in a docker system.

First step is to run the command docker compose up.
    - Make sure you are in the correct location when running the command
        (./Group6/Assets)

Let it run for a few minutes.
To check if eveything is running go to:
    - localhost:8080 - Kafka server UI
    - localhost:8081 - Spark server UI
    - localhost:8082 - Airflow UI

Second Step is running the producers
    - Since we have auto create topics you won't need to create it for Kafka
    - Make sure you install the python packages needed to run this file in
        virtual enviornment.

        Dependencies:
        pip install kafka-python faker

    Run the producers with this command:
    -- For more data increase the interval of creation

    python transaction_events_producer.py --bootstrap-servers localhost:9092 --topic transaction_events --interval 2.0
    python user_events_producer.py --bootstrap-servers localhost:9092 --topic user_events --interval 1.0

    Let this run for a few minutes to get a good chunk of data

The Third Step after running the producers is going back to the Airflow UI

Log in using admin, admin - Real life, make sure the passwords are way more secure

Then you would run the DAG in the airflow, by using the play button
    (The DAG is called streamflow_main)

When the DAG is running it creates
    - files in landing zone (BRONZE Zone) for the raw data
    - files in the gold zone for the cleaned business data

That would be all for our project currently.

If you wish to change what topics are listend to then change first/second topic in the DAG
If you want to make kafka listen for a longer amount of time then change time_duration in the DAG




