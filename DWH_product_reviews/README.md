# DWH
## _Designing a Data Warehouse and Orchestrating pipelines_

In this project I have Modelled a DWH to store Product, Rating & Reviews and data from ecommerce, and make it accessible for Analysis.

## Technical Overview

- Source Files are downloaded in Pipeline.
- Dimesnional Modelling.
- Airflow used for Orchestration 
- Postgres Database Storage.
- Jupyter Notebook for Analysis.
- Docker Runnable.

## Source Data used

For this I have used Amazon publicly avaiable datasets of Reviews and Product Metadata.

> More information about these datasetd can be found [here][data_link].

Airflow task is used to download files and Load them into STAGING Tables.
- A File can be processed parallelly, we need to configure new task and define `offset` and `limit` of records.
- Quality checks (removing null key rows and null charcters ) are also implemented. These can be expanded upon to add diffrent checks. 


## Dimensional Model

Below dimensional model is implemented in Postrges DB.

![alt text](https://github.com/Ajtmalik/public/blob/DWH_product_reviews/main/model.jpg?raw=true)

## Running solution

We use Docker to run this solution on any machine. Run `docker compose up` , it will spin up all required containers. 3 main service are:

- ## Airflow
```
Accessed at -   http://localhost:8080
Username :  airflow
Password : airflow
```

- ## Postrgres DB
DB can be accessed by any other tool also using below connection details.
```
Host : localhost
Database : mydb
Port : 5431
Uname : db_user
Password: pwd
```

- ## Jupyter Notebook
```
Access at – http://localhost:8888
Password :  copy token from jupyter container logs and login. 
```

=================================================================================================

**Free Software, Hell Yeah!**

[//]: #
   [data_link]: <http://jmcauley.ucsd.edu/data/amazon/links.html>
