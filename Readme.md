# GitHub Trends with Airflow and BigQuery
## Overview
This project integrates **Apache Airflow** with **Google BigQuery** to automate the aggregation of daily statistics from GitHub repositories and **Hacker News**. The workflow runs on Airflow using a Dockerized environment and leverages BigQuery for data storage and analysis. 

## Important Information

- **Project Source**: The code for this project is located in the **master branch**.
- **Documentation**: Comprehensive documentation can be found in the **main branch**.
## Key Features

- **Automated DAG** for aggregating daily GitHub repository and Hacker News statistics.
- **Docker-based setup** for easy deployment of Airflow with BigQuery integration.
- **Efficient data extraction** from GitHub and Hacker News.
- **Reliable and scalable pipeline** using Airflow's LocalExecutor and PostgreSQL as the metadata database.

## Prerequisites

To run this project, you will need the following:

- **Docker** and **Docker Compose** installed.
- **Google Cloud account** with access to **BigQuery** and relevant open-source datasets.
- **A service account (Cloud Console) with a Google Cloud JSON key** for authentication.

# Installation and Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/airflow-bigquery-project.git
   cd airflow-bigquery-project
   Run the Docker environment: docker-compose up
   Access Airflow UI: Once the services are up and running, access the Airflow UI at http://localhost:8080 to monitor and manage the DAGs.
2. **Set up the environment**
   **Google Cloud Service Key**:
   Go to the console:
   ![Create the service account step 1](img/service_account.png)
   ![Create the service account step 2](img/service_account2.png)
   ![Create the service account step 3](img/service_account3.png)
   ![Create the service account step 4](img/service_account4.png)
   ![Create the service account step 5](img/service_account5.png)
   ![Create the service account step 6](img/service_account6.png)
   Add your **Google Cloud JSON key file** to the ./keys directory.
   **airflow Connection:**
After having the GCP key, you need to create a connection in Admin -> Connections using your key.

In Airflow you need to define the google_cloud_default named connection to your project:
   ![airflow_connection](img/airflow_connection.png)
   **Enter the config variables**
   After connection has been set up, you can go to the bigquery_github_trends DAG, and enter the value of config variables in Admin -> Connections :
   ![airflow_variable](img/airflow_variable.png)
  - **BQ_PROJECT:** the bigquery project you are working on
  -**BQ_DATASET:** the bigquery dataset you are working on



   
