import os, sys, traceback
import datetime as dt
from graf_analysis.grafanaAnalysis import Analysis
from sosdb import Sos
import pandas as pd
import numpy as np
 
class cpuTSDSOS(Analysis):
    def __init__(self, cont, start, end, schema='job_id', maxDataPoints=4096):
        super().__init__(cont, start, end, schema, 1000000)

    def get_data(self, metrics, filters=[],params=None):
        try:
            procstat_metrics = ['user','nice','sys','idle','iowait','irq','softirq','steal','guest','guest_nice']
            sel = f'select {",".join(procstat_metrics)} from {self.schema}'
            where_clause = self.get_where(filters)
            order = 'time_job_comp'
            order = 'job_time_comp'
            orderby='order_by ' + order
            self.query.select(f'{sel} {where_clause} {orderby}')
            df = self.get_all_data(self.query)
            prods = sorted(pd.unique(df['component_id']).tolist())
            df['timestamp_int'] = df['timestamp'].astype('int')/1e9
            df['timestamp'] = df['timestamp_int'].astype('int')
            tmp = df[procstat_metrics]
            df['sum'] = tmp.sum(axis=1)
            df['sum'] = df.groupby('component_id')['sum'].diff().fillna(method='bfill')
            df['idle'] = df.groupby('component_id')['idle'].diff().fillna(method='bfill')
            df['perc'] = (df['sum']-df['idle'])/df['sum'] * 100
            df['timestamp'] = df['timestamp']*1000
            df = df[['timestamp','component_id','perc']]
            res = pd.pivot_table(df,values='perc',index='timestamp',columns='component_id', fill_value=np.nan).reset_index()
            res = res.interpolate(method='linear', limit_direction='forward', axis=0)
            return res
        except Exception as e:
            a, b, c = sys.exc_info()
            print(str(e)+' '+str(c.tb_lineno))
