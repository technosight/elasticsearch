from datetime import datetime, timedelta
import numpy as np


class ElasticsearchQueries:

    @classmethod
    def get_query_create_new_index(cls, es_index_name, columns, shards=1, replicas=1):
        '''
        Prepares a query in a form of dict.
        :param es_index_name: str
        :param columns: dict
        :param shards: int
        :param replicas: int
        :return: dict
        '''
        # convert columns into elasticsearch dict description
        properties = {}
        for key, value in columns.items():
            if value == np.datetime64:
                properties[key] = {
                    'type': 'date',
                    'format': 'strict_date_hour_minute_second'
                }
                continue
            if value == np.float64:
                properties[key] = {
                    'type': 'float'
                }
            if value == str:
                properties[key] = {
                    'type': 'text'
                }
                continue
            if value == np.int64:
                properties[key] = {
                    'type': 'integer'
                }
                continue
            if value == np.object_:
                properties[key] = {
                    'type': 'text'
                }

        index_settings = {
            'settings': {
                'number_of_shards': shards,
                'number_of_replicas': replicas
            },
            'mappings': {
                es_index_name: {
                    'dynamic': 'strict',
                    'properties': properties
                }
            }
        }

        return index_settings


    @classmethod
    def get_query_count_num_of_rows(cls, start_date, end_date=None):
        '''
        Prepares a query in a form of dict.
        :param start_date: datetime
        :param end_date:  datetime
        :return: dict
        '''
        if not end_date:
            # select data from 1 day only
            start_date = datetime(start_date.year, start_date.month, start_date.day)
            end_date = start_date + timedelta(days=1) - timedelta(seconds=1)

        query = {
            'query': {
                'range': {
                    'timestamp': {
                        'gte': start_date,
                        'lte': end_date
                    }
                }
            }
        }
        return query