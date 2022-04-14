import os, sys, traceback
import datetime as dt
from graf_analysis.grafanaAnalysis import Analysis
from numsos.DataSource import SosDataSource
from numsos.Transform import Transform
from sosdb.DataSet import DataSet
from sosdb import Sos,DSos
import pandas as pd
import numpy as np

class singleMetricRateTSDSOS(Analysis):
    def __init__(self, cont, start, end, schema='Lustre_Client', maxDataPoints=4096):
        super().__init__(cont, start, end, schema, 1000000)

    def get_data(self, metrics, filters=[],params=None):
        self.filters = filters
        self.sel = "select " + ",".join(metrics) + " from " + str(self.schema)
        self.sel = f'select {",".join(metrics)} from {self.schema}'
        self.orderby='order_by time_comp_job'
        self.orderby='order_by job_time_comp'
        try:
            nodetype = []
            if params is not None:
                chunks = params.split(',')
                for i in chunks:
                    if 'ds' in i:
                        self.ds = int(i.split('=')[1])
            else:
               self.ds = 1
            df = self.get_data_comp(metrics)
            return df
        except Exception as e:
            a, b, c = sys.exc_info()
            print(str(e)+' '+str(c.tb_lineno))

    def get_data_comp(self, metrics, timebool=False):
            where_clause = self.get_where(self.filters)
            self.query.select(f'{self.sel} {where_clause} {self.orderby}')
            df = self.get_all_data(self.query)
            # Downsample timeseries
            df['timestamp'] = df['timestamp'].astype(int)/1e9
            df['timestamp'] = df['timestamp'].astype(int)
            if self.ds != 1:
                df['time_ds'] = df['timestamp'].astype('int')%self.ds
                df = df[df['time_ds'] == 0]
                df = df.drop(['time_ds'],axis=1)
            # Sum all metrics together
            df['met_sum'] = df[metrics].sum(axis = 1)
            df = df.drop(metrics,axis=1)
            # Find the rate of metrics by dividing by the met and time diff for each comp_id
            df['met_diff'] = df.groupby(['component_id'])['met_sum'].diff().fillna(0)
            df['time_diff'] = df.groupby(['component_id'])['timestamp'].diff().fillna(0)
            df['met_rate'] = df['met_diff']/df['time_diff']
            df = df.fillna(0)
            # Sum rates together for final value
            ret = pd.pivot_table(df,values='met_rate',index='timestamp',columns='component_id', fill_value=0).reset_index()
            ret['timestamp'] = ret['timestamp']*1000
            return ret

