import os, sys, traceback
import datetime as dt
from graf_analysis.grafanaAnalysis import Analysis
from numsos.DataSource import SosDataSource
from numsos.Transform import Transform
from sosdb.DataSet import DataSet
from sosdb import Sos,DSos
import pandas as pd
import numpy as np
 
class compMinMeanMaxDSOS(Analysis):
    def __init__(self, cont, start, end, schema='meminfo', maxDataPoints=4096):
        query_block = 1000000
        super().__init__(cont, start, end, schema, query_block)

    def get_data(self, metrics, filters=[], params=None):
        metric = metrics[0]
        metrics = ['timestamp', metric]
        sel = "select " + ",".join(metrics) + " from " + str(self.schema)
        sel = f'select {",".join(metrics)} from {self.schema}'
        where_clause = self.get_where(filters)
        orderby = 'time_job_comp'
        orderby_filter='order_by ' + orderby
        self.query.select(f'{sel} {where_clause} {orderby_filter}')
        if params is not None:
            chunks = params.split(',')
            for i in chunks:
                if 'ds' in i:
                    self.ds = int(i.split('=')[1])
                else:
                    self.ds = 1
        else:
            self.ds = 1

        try:
            df = self.get_all_data(self.query)
            df['time_downsample'] = df['timestamp'].astype('int')/1e9
            df['time_downsample'] = df['time_downsample'].astype('int')%self.ds
            df = df[df['time_downsample'] == 0]
            df = df.drop(['time_downsample'],axis=1)
            df['timestamp'] = df['timestamp'].astype(int) / 1e9
            df['timestamp'] = df['timestamp'].astype(int)
            ret = pd.DataFrame(pd.unique(df['timestamp'].astype(int)*1000), columns=['timestamp'])
            ret['min'] = df.groupby(by=['timestamp'])[metric].min().reset_index()[metric]
            ret['mean'] = df.groupby(by=['timestamp'])[metric].mean().reset_index()[metric]
            ret['max'] = df.groupby(by=['timestamp'])[metric].max().reset_index()[metric]
            return ret
        except Exception as e:
            a, b, c = sys.exc_info()
            print(str(e)+' '+str(c.tb_lineno))
