import os, logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class ageDeath(beam.DoFn):
  def process(self, element):
    artist_record = element
    
    print("current artist: " + artist_record.get("DisplayName"))
    splitData = artist_record.get("ArtistBio").split(",")
    nationality = splitDate[0]
    born = splitDate[1]
    if "-" in born:
        born = born.split('-')[0]

    artist_record["nationality"] = nationality
    artist_record["born"] = born

    return [artist_record]


def run():
    PROJECT_ID = 'crypto-pulsar-266623' # change to project id

    options = {
    'project': PROJECT_ID
    }
    opts = beam.pipeline.PipelineOptions(flags=[], **options)

    # Create beam pipeline using local runner
    p = beam.Pipeline('DirectRunner', options=opts)

    sql = 'SELECT * FROM artists.artist'
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
    
    # writing input to log
    query_results | 'Write log 1 - query table' >> WriteToText('input.txt')

    # counting 
    artistFormated = query_results | 'Format artist bio' >> beam.ParDo(ageDeath())

    # wrting output to log
    states | 'Write log 2 - output table' >> WriteToText('output.txt')

    dataset_id = 'artists'
    table_id = 'artist_beam'
    schema_id = 'ConstituentID:INTEGER ,DisplayName:STRING , ArtistBio:STRING, Gender:STRING'

    # write PCollection to new BQ table
    states | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
                                                  table=table_id, 
                                                  schema=schema_id,
                                                  project=PROJECT_ID,
                                                  create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                  write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                  batch_size=int(100))
         
    
    result = p.run()
    result.wait_until_finish() 

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()