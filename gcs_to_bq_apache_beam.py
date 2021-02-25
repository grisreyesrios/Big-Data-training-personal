from __future__ import absolute_import

import argparse
import sys
import apache_beam as beam
from apache_beam.io.avroio import ReadFromAvro
from apache_beam.io.avroio import WriteToAvro
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.internal.clients import bigquery
import avro
import json
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter


PROJECT= 'lively-armor-283518'
BUCKET= 'dataflow-excercise-bigquery'


table_schema = bigquery.TableSchema()

def run():
   argv = [
      '--project={0}'.format(PROJECT),
      '--staging_location=gs://{0}/staging/'.format(BUCKET),
      '--temp_location=gs://{0}/staging/'.format(BUCKET),
      '--runner=DataflowRunner'
   ]

   #Apache Beam Pipeline

   p = beam.Pipeline(argv=argv)

   (p
      | 'ReadAvroFromGCS' >> ReadFromAvro('gs://dataflow-excercise/test-dataset.avro')
      | 'WriteToBigQuery' >> beam.io.WriteToBigQuery('{0}:apache_beam.avro_dataflow2'.format(PROJECT), schema=table_schema)
   )

   p.run()

if __name__ == '__main__':
    run()