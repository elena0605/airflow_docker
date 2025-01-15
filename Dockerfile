FROM apache/airflow:2.10.3

# Install pymongo and other dependencies
RUN pip install pymongo
RUN pip install neo4j
RUN pip install apache-airflow-providers-mongo
RUN pip install apache-airflow-providers-neo4j

