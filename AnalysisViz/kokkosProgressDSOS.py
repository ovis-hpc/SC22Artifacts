import os, sys, traceback
import datetime as dt
from graf_analysis.grafanaAnalysis import Analysis
from numsos.DataSource import SosDataSource
from numsos.Transform import Transform
from sosdb.DataSet import DataSet
from sosdb import Sos 
import pandas as pd
import numpy as np
import statistics as stat
 
class kokkosProgressDSOS(Analysis):
    def __init__(self, cont, start, end, schema='kokkos', maxDataPoints=4096):
        query_block = 1000000
        super().__init__(cont, start, end, schema, query_block)

    def get_data(self, metrics, filters=[], params=None):
        metrics = ['name','rank','total_kernel_count']
        sel = "select " + ",".join(metrics) + " from " + str(self.schema)
        sel = "select " + ",".join(metrics) + " from " + str(self.schema)
        sel = f'select {",".join(metrics)} from {self.schema}'
        where_clause = self.get_where(filters)
        orderby = 'job_time_rank'
        orderby_filter='order_by ' + orderby
        self.query.select(f'{sel} {where_clause} {orderby_filter}')
        #where = " where (timestamp > " + str(float(self.start)) + ") and (timestamp < " \
        #        + str(float(self.end)) + ") and (job_id == " + str(job_id) + ")"
        try:
            df = self.get_all_data(self.query).reset_index(drop=True)
            ret = self.get_progressts(df)
            return ret
        except Exception as e:
            a, b, c = sys.exc_info()
            print(str(e)+' '+str(c.tb_lineno))
    
    def get_progressts(self, df):
        try:
           df['time_minutes'] = df['timestamp'].astype(int)/60
           df['time_minutes'] = df['time_minutes'].astype(int)
           tmp = df.groupby(by=['time_minutes','rank'],sort=False)['total_kernel_count'].max() - \
                 df.groupby(by=['time_minutes','rank'],sort=False)['total_kernel_count'].min()
           tmp = tmp.reset_index()
           tmp = tmp.groupby('time_minutes')['total_kernel_count'].sum()
           ret = pd.DataFrame(columns=['timestamp','function_per_minute'])
           ret['timestamp'] = tmp.index * 1000 * 60 + 60000
           ret['function_per_minute'] = tmp.values 
        except Exception as e:
            a, b, c = sys.exc_info()
            print(str(e)+' '+str(c.tb_lineno))
        return ret

