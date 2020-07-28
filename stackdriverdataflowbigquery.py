# Copyright 2020 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START parsing Stackdriver logs from pubsub_to_bigquery]
import argparse
import logging
import json
import re
import ast

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions

def iterate_multidimensional(my_dict):
    return_json = {
        "insertId": None,
        "logName": None,
        "receiveTimestamp": None,
        "textPayload": None,
        "timestamp": None,
        "trace": None
    }
    for k,v in my_dict.items():
        if(isinstance(v,dict)):
            iterate_multidimensional(v)
            continue
        if k in return_json:
            return_json[k] = v
    return return_json 

def iterate_textpayload_multidimensional(my_dict):
    return_dict = {
        "error_type": None,
        "session_id": None,
        "caller_id": None,
        "email": None,
        "code": None,
        "string_value": None,
        "lang": None,
        "speech": None, 
        "is_fallback_intent": None,
        "webhook_for_slot_filling_used": None,
        "webhook_used": None,
        "intent_name": None,
        "intent_id": None,
        "score": None,
        "action": None,
        "resolved_query": None,
        "source": None
    }

    for k,v in my_dict.items():
        if(isinstance(v,dict)):
            iterate_textpayload_multidimensional(v)
            continue
        if k in return_dict:
            return_dict[k] = v
    return return_dict

def iterate_textpayload(my_list):
    res = []
    for item in my_list:
        my_list_item = item.replace('"', '')
        if ':' in my_list_item:
            res.append(map(str.strip, my_list_item.split(":", 1)))
    return dict(res)

# function to get response body data from pub/sub message and build structure for BigQuery load
def parse_transform_response(data):
    logging.info('--- START parse_transform_response Function ---')
    pub_sub_data = json.loads(data)
    fullpayload_dict = iterate_multidimensional(pub_sub_data)
    # Clean textPlayload from Stackdriver - not a valid JSON object
    text_payload = fullpayload_dict['textPayload']
    return_merged_payload = None

    if text_payload != None:
        regex = re.compile(r'''[\S]+:(?:\s(?!\S+:)\S+)+''', re.VERBOSE)
        matches = regex.findall(pub_sub_data["textPayload"])
        iterate_textpayload_response = iterate_textpayload(matches)
        textpayload_dict = iterate_textpayload_multidimensional(iterate_textpayload_response)
        if textpayload_dict["error_type"] is not None:
            textpayload_dict["error_type"] = textpayload_dict["error_type"].replace("\n", "").replace("}", "").strip()
        return_merged_payload = dict(list(fullpayload_dict.items()) + list(textpayload_dict.items()))
    if return_merged_payload is not None:
        logging.info('--- END parse_transform_response Function ---')
        logging.info(return_merged_payload)
        return return_merged_payload
    else:
        logging.info('--- END parse_transform_response Function ---')
        logging.info(fullpayload_dict)
        return fullpayload_dict

def run(argv=None, save_main_session=True):
    """Build and run the pipeline."""
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--input_topic',
        help=('Input PubSub topic of the form '
              '"projects/<PROJECT>/topics/<TOPIC>".'))
    group.add_argument(
        '--input_subscription',
        help=('Input PubSub subscription of the form '
              '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'))
    parser.add_argument('--output_bigquery', required=True,
                        help='Output BQ table to write results to '
                             '"PROJECT_ID:DATASET.TABLE"')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    pipeline_options.view_as(StandardOptions).streaming = True
    p = beam.Pipeline(options=pipeline_options)

    # Read from PubSub into a PCollection.
    if known_args.input_subscription:
        messages = (p
                    | beam.io.ReadFromPubSub(
                    subscription=known_args.input_subscription)
                    .with_output_types(bytes))
    else:
        messages = (p
                    | beam.io.ReadFromPubSub(topic=known_args.input_topic)
                    .with_output_types(bytes))

    decode_messages = messages | 'DecodePubSubMessages' >> beam.Map(lambda x: x.decode('utf-8'))

    # Parse response body data from pub/sub message and build structure for BigQuery load
    output = decode_messages | 'ParseTransformResponse' >> beam.Map(parse_transform_response)

    # Write to BigQuery
    bigquery_table_schema = {
        "fields": [
                    {
                        "mode": "NULLABLE",
                        "name": "session_id",
                        "type": "STRING"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "trace",
                        "type": "STRING"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "caller_id",
                        "type": "STRING"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "email",
                        "type": "STRING"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "timestamp",
                        "type": "TIMESTAMP"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "receiveTimestamp",
                        "type": "TIMESTAMP"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "resolved_query",
                        "type": "STRING"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "string_value",
                        "type": "STRING"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "speech",
                        "type": "STRING"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "is_fallback_intent",
                        "type": "STRING"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "webhook_for_slot_filling_used",
                        "type": "STRING"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "webhook_used",
                        "type": "STRING"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "intent_name",
                        "type": "STRING"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "intent_id",
                        "type": "STRING"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "score",
                        "type": "STRING"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "action",
                        "type": "STRING"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "source",
                        "type": "STRING"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "error_type",
                        "type": "STRING"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "code",
                        "type": "STRING"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "insertId",
                        "type": "STRING"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "logName",
                        "type": "STRING"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "lang",
                        "type": "STRING"
                    },
                    {
                        "mode": "NULLABLE",
                        "name": "textPayload",
                        "type": "STRING"
                    }
                    
                ]
    }
    output | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            known_args.output_bigquery,
            schema=bigquery_table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

    p.run()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    run()
# [END parsing Stackdriver logs from pubsub_to_bigquery]