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
pd.set_option('use_inf_as_na', True)
pd.set_option('display.float_format','{:.10f}'.format)
 
class kokkosFuncTableDSOS(Analysis):
    def __init__(self, cont, start, end, schema='kokkos', maxDataPoints=4096):
        query_block = 1000000
        super().__init__(cont, start, end, schema, query_block)

    def get_data(self, metrics, filters=[], params=None):
        metrics = ['name','rank','total_kernel_count','current_kernel_time','current_kernel_count']
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
            df = df.sort_values(['timestamp'])
            ret = self.get_function_table(df)
            return ret
        except Exception as e:
            a, b, c = sys.exc_info()
            print(str(e)+' '+str(c.tb_lineno))
    
    def get_function_table(self, df):
        tmp = df[df['rank'] == 0]['total_kernel_count'].values
        #srate = tmp[1] - tmp[0]
        srate = 100
        df['current_kernel_count'] = df['current_kernel_count'].astype(int)
        df['current_kernel_time'] = df['current_kernel_time'].astype(float)
        ret = df[['name']]
        ret = ret.drop_duplicates()
        tmp1max = df.groupby(['name','rank'],sort=False)['current_kernel_count'].max().reset_index()
        tmp1min = df.groupby(['name','rank'],sort=False)['current_kernel_count'].min().reset_index()
        tmp1max['current_kernel_count'] =  tmp1max['current_kernel_count'] - tmp1min['current_kernel_count']
        tmp1max=tmp1max.replace({'current_kernel_count': {0: 1}}) 
        tmp = tmp1max.groupby(['name'],sort=False)['current_kernel_count'].sum().values*srate
        ret['kernel_count'] = tmp
        ret['kernel_time'] = df.groupby(['name'],sort=False)['current_kernel_time'].sum().values*srate
        ret['time_per_call'] = ret['kernel_time'] / ret['kernel_count']
        ret['time_per_call'] = ret['time_per_call'].fillna(0)
        total_time = ret['kernel_time'].sum()
        total_count = ret['kernel_count'].sum()
        ret['perc_kernel_count'] = ret['kernel_count']/total_count * 100
        ret['perc_kernel_time'] = ret['kernel_time']/total_time * 100
        return ret

