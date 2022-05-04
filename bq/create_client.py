from google.cloud import bigquery
import os
from pprint import pprint

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = \
    '/home/archil/Desktop/bq_sa_credentials.json'

CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

client = bigquery.Client()

dataset_objects = list(client.list_datasets())

datasets = [client.get_dataset(dataset.dataset_id) for dataset in
            dataset_objects]

project = client.project


def fetch_metadata(request):
    main_dict = {}
    tbls_dict = {}
    dataset_list = []

    for dataset in datasets:

        if dataset.dataset_id.startswith('Meta'):
            continue

        tables = client.list_tables(dataset.dataset_id)

        dataset_tables = []

        for tbl in tables:
            table = client.get_table(f'{dataset.dataset_id}.{tbl.table_id}')

            table_dict = {
                'ID': table.table_id,
                'Dataset': table.dataset_id,
                'Partitioning Type': table.partitioning_type,
                'Created': table.created.strftime("%m/%d/%Y, %H:%M:%S"),
                'Modified': table.modified.strftime("%m/%d/%Y, %H:%M:%S"),
                'Description': table.description,
                'Location': table.location,
                'Schema': table.schema,
                'Records': table.num_rows,
                'Expiration': table.expires,
                'Project': table.project
            }

            dataset_tables.append(table_dict)

        if request['Tables']:


        dataset_dict = {
            'ID': dataset.dataset_id,
            'Location': dataset.location,
            'Created': dataset.created.strftime("%m/%d/%Y, %H:%M:%S"),
            'Modified': dataset.modified.strftime("%m/%d/%Y, %H:%M:%S"),
            'Description': dataset.description,
            'Lables': dataset.labels,
            'Tables': dataset_tables
        }

        dataset_list.append(dataset_dict)

        main_dict['DatasetDetails'] = dataset_list

# pprint(main_dict)