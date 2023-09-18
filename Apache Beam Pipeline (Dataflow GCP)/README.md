# DATAFLOW - apache beam
## _Creating an advance apache beam bulk pipeline and running it on GCP Dataflow service_

We will use ```bigquery-public-data.london_bicycles``` dataset to find total distance (strainght line) travelled between any two stations.

## Setting Up Environment
 
Before starting a GCP project must be setup and Dataflow Api services enabled. Optionally a Service Account can also be created and private key downloaded to be used for authentication.

- Navigate to project directory

We will set up python environment using the ```setup.py``` file available.
```python -m venv env```

Activate Python Virtual Environment.
```.\env\Scripts\activate```

Install required packages.
```pip install .  ```

Setup GCP authentication.

Either export below environment variable with path to service acoount key
```set GOOGLE_APPLICATION_CREDENTIALS=Path\to\key.json```

OR run command in cli
``` gcloud auth login``` GCP CLI installation is needed to run this command

## Run the pipeline on local machine

> The ```Dataflow.py``` file configured to run locally 

Update Project ID with a Valid GCP project ID (Dataflow API Services enabled).
Before executing the ```gcs_location``` needs to be updated with a valid GCS bucket path.
And update ```qry_b``` with a valid Where clause so that whole data is not processed locally.

Run command
``` python -m Dataflow```

Once the Pipeline is successfully tested we can run it on dataflow.
The code can also be updated to your processing needs, Below features are implemented in this.
- Read from Big Query and Store to GCS location.
- String manipulation.
- Aggregation.
- Join & Aggregation across 2 pcollection.
- Use of side inputs to perform join which is similar to Broasdcast in Spark.  

## Run on GCP 🔥

We will use 3 runners by default for executing our pipeline.

Update ```gcs_location``` with a valid GCS bucket path in below config.
Update ```setup.py``` location in below cofig

And update the config to below 
```
options = {'project': 'your_gcp_project_ID',
           'runner': 'DataflowRunner',
           #'runner': 'DirectRunner', #To run Locally
           #'direct_num_workers': 1,
           #'direct_running_mode': 'multi_processing',
           'region': 'europe-west1',
           'num_workers': 3,
           'gcs_location': 'gs://gcs_path/',
           'temp_location': 'gs://gcs_path/temp/',
           'staging_location': 'gs://gcs_path/staging/',
           'setup_file': 'project/path/setup.py'
           }
```




Run command
``` python -m Dataflow```


> Job status can be checked on GCP console.

> Output is formated as 
"start_station_id,stop_station_id, num_of_rides, total_distance"



Enjoy!!
