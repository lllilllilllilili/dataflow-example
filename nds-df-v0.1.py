#!/usr/bin/env python3

# Copyright 2016 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import apache_beam as beam
import csv
import os
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json


class DataTransformation:
    def __init__(self):
        dir_path = os.path.abspath(os.path.dirname(__file__))
        self.schema_str = ""
        schema_file = os.path.join(dir_path, "schema", "mv_sa_tran_online.json")
        with open(schema_file) \
                as f:
            data = f.read()
            #self.schema_str = data
            self.schema_str = '{"fields": ' + data + '}'

def create_row(fields):
    header = 'sale_date,store_code,register_id,voucher_id,voucher_seq,product_code,sale_qty,sale_amount,sale_vat_amount,profit_amount,customer_code,mall_product_code,mall_flag,order_channel,mall_com_id,deli_type,order_id,mall_order_flag,mall_deli_flag,mall_order_cnt_flag,mall_deli_cnt_flag,rece_address,rece_detail_address,customer_cnt,order_cnt,post_id,deli_order,order_deli_date,sale_flag,org_order_code,picker_id,sale_hour,sold_stock_flag,card_name,payment_flag,vendor_code,purchase_type,sale_channel_flag,basic_amount,sold_stock_qty,order_code,real_order_flag,order_name,class_code,order_wanted_deli_date,region_id,picking_sold_flag,deli_id,coupon_flag,picking_sta_date,picking_end_date,order_deli_sta_date,order_deli_end_date,picking_time,order_time,reserve_date,pro_option_value,order_deli_method,dong_code,offline_order_flag,add_order_org_order_code,deli_start_date,original_code,reserve_deli_order,voucher_code1,voucher_code2,group_region_id,member_code,sold_amount,replace_amount'.split(
        ',')


    featdict = {}


    for name, value in zip(header, fields):
        featdict[name] = value


    return featdict


def run(project, bucket, dataset):
    argv = [
        '--project={0}'.format(project),
        '--job_name=nds-dataflow-job',
        '--save_main_session',
        '--staging_location=gs://df-workspace/df_staging/',
        '--temp_location=gs://df-workspace/df_temp/',
        #'--template_location=gs://alex_dataflow_template/MyTemplate',
        '--runner=Dataflow' #Dataflow or DirectRunner
    ]
    nds_filename = 'gs://megamart-data/tr/MV_SA_TRAN_ONLINE20190101.csv'

    events_output = '{}:{}.mv_sa_tran_online2'.format(project, dataset)

    pipeline = beam.Pipeline(argv=argv)




    Input = (pipeline
                | 'nds:read' >> beam.io.ReadFromText(nds_filename, skip_header_lines=1)
                | 'nds:fields' >> beam.Map(lambda line: next(csv.reader([line])))

                )

    data_ingestion = DataTransformation()
    schema = parse_table_schema_from_json(data_ingestion.schema_str)
    additional_bq_parameters = {
        "allowJaggedRows": True,
        "allowQuotedNewlines": True,
    }
    
    (Input
     | 'nds:totablerow' >> beam.Map(lambda fields: create_row(fields))
     | 'nds:out' >> beam.io.WriteToBigQuery(
                events_output, schema=schema,
                additional_bq_parameters=additional_bq_parameters,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
     )

    pipeline.run()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Run pipeline on the cloud')
    parser.add_argument('-p', '--project', help='Unique project ID', required=True)
    parser.add_argument('-b', '--bucket', help='Bucket where your data were ingested', required=True)
    parser.add_argument('-d', '--dataset', help='BigQuery dataset', default='flights')
    args = vars(parser.parse_args())

    print("Correcting timestamps and writing to BigQuery dataset {}".format(args['dataset']))

    run(project=args['project'], bucket=args['bucket'], dataset=args['dataset'])
