FROM apache/airflow:2.10.3

# Install pymongo and other dependencies
RUN pip install pymongo
