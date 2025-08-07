FROM apache/airflow:3.0.0

# Install pymongo and other dependencies in a single RUN to reduce image layers
RUN pip install --no-cache-dir \
    pymongo \
    neo4j \
    apache-airflow-providers-mongo \
    apache-airflow-providers-neo4j \
    apache-airflow-providers-standard \
    google-auth \
    google-api-python-client \
    google-auth-oauthlib \
    google-auth-httplib2



