from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import elasticsearch.helpers

import pandas as pd
from pathlib import Path
import pickle
import logging
from functools import wraps
from datetime import datetime
import hashlib

from es_queries import ElasticsearchQueries

# set config parameters
cache_dir = Path('C:\tmp\pickle')
cache_dir.mkdir(parents=True, exist_ok=True)
caching_on = True
request_timeout=30

# initialise logger
logger = logging.getLogger('elasticsearch_connector')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
# console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)


def cacheable(method):
    '''
    Decorates methods that benefit data caching
    :param method:
    :return:
    '''
    def wrapper(*args, **kwargs):
        if not caching_on:
            return method(*args, **kwargs)

        es_index_name = kwargs.get('es_index_name')

        # convert datetimes to strings
        start_date = kwargs.get('start_date') or 'EmptyStartDate'
        if isinstance(start_date, datetime):
            start_date = start_date.strftime('%Y-%m-%d')
        end_date = kwargs.get('end_date') or 'EmptyEndDate'
        if isinstance(end_date, datetime):
            end_date = end_date.strftime('%Y-%m-%d')

        # convert list of dicts to a list of tuples
        query_params = []

        query_params_must = kwargs.get('query_params_must')
        if isinstance(query_params_must, list):
            for i in query_params_must:
                query_params.extend([(key, value) for key, value in i.items()])

        query_params_must_not = kwargs.get('query_params_must_not')
        if isinstance(query_params_must_not, list):
            query_params.extend([(key, value) for key, value in query_params_must_not])

        # build unique cache file name for given method parameters
        cache_fname = '{}_{}_from_{}_to_{}'.format(method.__name__, es_index_name, start_date, end_date)

        if len(query_params) > 0:
            # sort tuples for checksums consistency
            query_params.sort()
            query_params = hashlib.md5(str(query_params).encode('utf-8')).hexdigest()
            cache_fname += '_{}'.format(query_params)

        result = None
        cache_fpath = cache_dir / '{}.pickle'.format(cache_fname)
        if cache_fpath.is_file():
            with open(cache_fpath, 'rb') as input:
                result = pickle.load(input)
                logger.debug('located cached data in {}'.format(cache_fpath))
                return result
        else:
            result = method(*args, **kwargs)
            with open(cache_fpath, 'wb') as output:
                pickle.dump(result, output, pickle.HIGHEST_PROTOCOL)
                logger.debug('cached data into {}'.format(cache_fpath))
        return result
    return wrapper


class ElasticsearchConnector:

    def __init__(self, host, port):
        self.es = Elasticsearch([
            {
                'host': host,
                'prot': port
            }
        ])


    def create_es_index(self, es_index_name, column_types):
        '''
        Create new elasticsearch index.
        :param es_index_name: str
        :param column_types: list
        :return: None
        '''
        logger.debug('creating es index: {}'.format(es_index_name))
        body = ElasticsearchQueries.get_query_create_new_index(
            es_index_name=es_index_name,
            columns=column_types
        )
        try:
            result = self.es.indices.create(
                index=es_index_name,
                ignore=400,
                body=body
            )
            if not result.get('acknowledge'):
                logger.error('failed to create index: {}'.format(es_index_name))
                return False

            logger.debug('create index: {}'.format(es_index_name))
            return True

        except Exception as e:
            logger.error('failed to create index: {}'.format(es_index_name))
            return False


    def count_num_of_rows(self, es_index_name, start_date, end_date):
        '''
        Counts a number of rows selected for given date range.
        :param es_index_name: str
        :param start_date: datetime
        :param end_date: datetime
        :return: int
        '''
        body = ElasticsearchQueries.get_query_count_num_of_rows(
            start_date=start_date,
            end_date=end_date
        )
        try:
            kwargs = {
                'index': es_index_name,
                'body': body
            }
            result = self.es.count(**kwargs)
            return result.get('count')
        except Exception as e:
            logger.error('failed to count number of rows in index: {}'.format(es_index_name))
            raise e


    def put_df(self, es_index_name, doc_type, df, update=False):
        '''
        Saves given dataframe in elasticsearch.
        :param es_index_name: str
        :param doc_type: str
        :param df: pandas.DataFrame
        :param update: Boolean
        :return: Boolean
        '''
        # create elasticsearch index if it doesn't exist yet
        if not self.es.indices.exists(es_index_name):
            logger.debug('creating es index: {}'.format(es_index_name))
            index_dtypes = df.dtypes.to_dict()
            index_column_types = dict(map(lambda x: (x, index_dtypes[x].type), index_dtypes))
            result = self.create_es_index(
                es_index_name=es_index_name,
                column_types=index_column_types
            )
            if not result:
                logger.error('failed to save df in es index: {}'.format(es_index_name))
                return False

            # check if the database
            # TODO: the code below needs to be reviewed as it depends on given requirements
            # TODO: use 'update' flag foor decision if existing rows would be updated or left intact

            # save given dataframe
            logger.debug('inserting records into es index: {}'.format(es_index_name))

            docs = df.to_dict(orient='records')
            result = bulk(
                client=self.es,
                actions=docs,
                index=es_index_name,
                doc_type=doc_type,
                raise_on_error=True,
                stats_only=True,
                max_retries=3,
                request_timeout=request_timeout
            )
            # TODO: consider an option to use parallel bulk
            # for success, info in parallel_bulk(lient=self.es, actions=docs):
            #     # TODO: implement parallel_bulk

            if result[1] != 0:
                logger.error('failed to save df in es index: {}'.format(es_index_name))
                return False
            return True


    @cacheable
    def get_df(self,
               es_index_name,
               doc_type,
               start_date,
               end_date=None,
               query_params_must=None,
               query_params_must_not=None,
               columns=None):
        '''
        Retrieves a dataframe from elasticsearch for given dates and optionl query parameters.
        Parameters in elasticsearch are used as follows:
        (param_name_11=value_11 AND param_name_12=value_12 AND ...) OR
        (param_name_21=value_21 AND param_name_22=value_22 AND ...) OR ...
        :param es_index_name: str
        :param doc_type: str
        :param start_date: datetime
        :param end_date: datetime
        :param query_params_must: list of dicts such as:
        [
            {param_name_11: value_11, param_name_21=value_21, ...},
            {param_name_21: value_21, param_name_22=value_22, ...},
            ...
        ]
        :param query_params_must_not: list of tuples such as:
        [
            (param_name_1, value_1),
            (param_name_2, value_2),
            ...
        ]
        :param columns: list
        :return: pandas.DataFrame
        '''
        if not self.es.indices.exists(es_index_name):
            logger.debug('failed to get data from es index: {}'.format(es_index_name))
            return pd.DataFrame(columns=columns)

        filters = []
        if start_date and end_date:
            filters.append(
                {
                    'range': {
                        # 'timestamp' column must exist in given es index
                        'timestamp': {
                            'gte': start_date,
                            'lte': end_date
                        }
                    }
                }
            )

        if query_params_must:
            must_params = []
            for param in query_params_must:
                must_param = [{'match': {name: value}} for name, value in param.items()]
                must_params.append(must_param)

            should_filters = [
                {
                    'bool': {
                        'must': item
                    }
                } for item in must_params
            ]

            if query_params_must_not:
                must_not_params = [{'match': {name, value}} for name, value in query_params_must_not]
                for filter in should_filters:
                    filter['bool']['must_not'] = must_not_params

            filters.append(
                {
                    'bool': {
                        'should': should_filters
                    }
                }
            )

        query = {
            'query': {
                'bool': {
                    'filter': filters
                },
            },
        }

        if columns:
            query['_source'] = columns

        result = elasticsearch.helpers.scan(
            client=self.es,
            index=es_index_name,
            doc_type=doc_type,
            preserve_order=True,
            query=query,
            request_timeout=request_timeout
        )
        i = 0
        data_holder = []
        for item in result:
            try:
                data_holder.append(item['_source'])
                i += 1
            except Exception as e:
                logger.error('failed at iteration {} on item {} with message: {}'.format(i, item, e))
                raise(e)
        out = pd.DataFrame(data_holder)
        return out