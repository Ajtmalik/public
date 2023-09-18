import logging
import argparse
import time


import apache_beam as beam
from geopy.distance import distance


options = {'project': 'inductive-seat-394607',
           #'runner': 'DataflowRunner',
           'runner': 'DirectRunner', #To run Locally
           'direct_num_workers': 1,
           'direct_running_mode': 'multi_processing',
           'region': 'europe-west1',
           #'num_workers': 3,
           'gcs_location': 'gs://gcs_path/',
           'temp_location': 'gs://gcs_path/temp/',
           'staging_location': 'gs://gcs_path/staging/',
           #'setup_file': 'project/path/setup.py'
           }
pipeline_options = beam.pipeline.PipelineOptions(flags=[], **options)


qry_s="SELECT id, latitude, longitude FROM `bigquery-public-data.london_bicycles.cycle_stations`"
qry_b="SELECT rental_id,start_station_id,end_station_id FROM `bigquery-public-data.london_bicycles.cycle_hire`"


class station_info(beam.DoFn):
  """Process each rental record :- join with station info & calculate ride distance"""
  
  def process(self, element, stations):
      from geopy.distance import distance
      try:
          start_coordinate=stations[element['start_station_id']]
          end_coordinate=stations[element['end_station_id']]
          cal_dis=distance(start_coordinate,end_coordinate).km
          return [{'rental_id':element['rental_id'],'start_station_id':element['start_station_id'],'end_station_id':element['end_station_id'],'distance':cal_dis}]
      except Exception as e:
          #print("The error is   :",e)
          pass

def run(argv=None, save_main_session=True):

    p = beam.Pipeline(options = pipeline_options)
    raw_stations_data = p | "Read stations raw data" >> beam.io.ReadFromBigQuery(query = qry_s,use_standard_sql=True) 
    ref_stations_data = raw_stations_data | "Create coordinates" >> beam.Map(lambda x: (x['id'],(str(x['latitude'])+' , '+str(x['longitude']))))
    bike_hire_data = p | "Read bike hire data" >> beam.io.ReadFromBigQuery(query = qry_b,use_standard_sql=True)
    final_joined_data = bike_hire_data | "Join data" >> beam.ParDo(station_info(), stations=beam.pvalue.AsDict(ref_stations_data))

    final_joined_data_res=final_joined_data | beam.Reshuffle()

    ride_cnt = (final_joined_data_res
                | "generate final keys and pair with one" >> beam.Map(lambda x: ((x['start_station_id'],x['end_station_id']),1))
                | "ride counts per key" >> beam.CombinePerKey(sum)
                )

    ride_distance = (final_joined_data_res
                | "generate final keys for distance" >> beam.Map(lambda x: ((x['start_station_id'],x['end_station_id']),x['distance']))
                | "total distance" >> beam.CombinePerKey(sum)
                )

    final_results = (({
                    'cnt': ride_cnt, 'distance': ride_distance
                    })
                    | 'Merge Data' >> beam.CoGroupByKey()
                    | 'Format Data' >> beam.Map(lambda x: str(x[0][0])+','+str(x[0][1])+','+str(x[1]['cnt'][0])+','+str(x[1]['distance'][0]))
                    )


    #final_results | "Write to GCS" >> beam.io.WriteToText('gs://gcs_path/output/'+'bike_output')
    final_results | "Write to GCS" >> beam.io.WriteToText('output/bike_output')  #To Store output in Local Storage.
    p.run()




if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  print('Start time:',time.ctime(time.time()))
  run()
  print('End time:',time.ctime(time.time()))
