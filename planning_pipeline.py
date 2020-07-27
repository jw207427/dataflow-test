from __future__ import absolute_import
from past.builtins import unicode
import argparse
import logging
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os
import json

# load the Service Account json file to allow GCP resources to be used
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'service/mktg-test-data-flow.json'

input_file = 'gs://mktg-data-bucket-1/planning.csv'


class DataIngestion:

    # this method parses the input csv and converts into a BigQuery-savable dictionary
    def parse_method(self, string_input):
        values = re.split(",",
                          re.sub('\r\n', '', re.sub(u'"', '', string_input)))
        row = dict(
            zip(('fiscal', 'productgroup', 'market', 'state', 'region',
                 'department', 'channel', 'month', 'quarter', 'event', 'cost', 'kpi'),
                values))
        return row


class CsvToJson(beam.DoFn):

    def process(self, string_input):
        values = re.split(",",
                          re.sub('\r\n', '', re.sub(u'"', '', string_input)))
        row = dict(
            zip(('fiscal', 'productgroup', 'market', 'state', 'region',
                 'department', 'channel', 'month', 'quarter', 'event', 'cost', 'kpi'),
                values))
        print(row)
        return [row]



def run(argv=None):
    """The main function which creates the pipeline and runs it."""

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Input file to read. This can be a local file or '
             'a file in a Google Storage Bucket.',
        default=input_file)

    parser.add_argument('--output',
                        dest='output',
                        required=False,
                        help='Output BQ table to write results to.',
                        default='planning.yoy')

    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)

    # DataIngestion is a class we built in this script to hold the logic for
    # transforming the file into a BigQuery table.
    data_ingestion = DataIngestion()

    # Initiate the pipeline using the pipeline arguments passed in from the
    # command line. This includes information such as the project ID and
    # where Dataflow should store temp files.
    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    (
            p | 'Read File from GCS' >> beam.io.ReadFromText(known_args.input,
                                                             skip_header_lines=1)

            # This stage of the pipeline translates from a CSV file single row
            # input as a string, to a dictionary object consumable by BigQuery.
            # It refers to a function we have written. This function will
            # be run in parallel on different workers using input from the
            # previous stage of the pipeline.
            # | 'String To BigQuery Row' >> beam.Map(lambda s: data_ingestion.parse_method(s))
            | 'Convert to Json' >> beam.ParDo(CsvToJson())
            | 'Write to BigQuery' >> beam.io.Write(
                beam.io.WriteToBigQuery(
                    # The table name is a required argument for the BigQuery sink.
                    # In this case we use the value passed in from the command line.
                    'planning.yoy',
                    #known_args.output,
                    # Here we use the simplest way of defining a schema:
                    # fieldName:fieldType

                    schema='fiscal:STRING,productgroup:STRING,market:STRING,state:STRING,' +
                           'region:STRING,department:STRING,channel:STRING,month:INTEGER,' +
                           'quarter:INTEGER,event:INTEGER,cost:FLOAT,kpi:FLOAT',

                    # Creates the table in BigQuery if it does not yet exist.
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    # Deletes all data in the BigQuery table before writing.
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
