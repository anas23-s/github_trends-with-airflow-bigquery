FROM apache/airflow:2.6.0

# Install the Google provider for Airflow
RUN pip install apache-airflow-providers-google
