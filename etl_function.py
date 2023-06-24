# -*- coding: utf-8 -*-
"""
Created on Sun May  7 17:37:44 2023

@author: chang
"""

import requests
from lxml import html
import pandas as pd
import numpy as np
import logging
from pandas.tseries.offsets import MonthEnd
from tqdm import tqdm
from datetime import date

pd.options.mode.chained_assignment = None
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ExtractTool():
    
    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self._started = " data extraction started."
        self._completed = " data extraction completed."
        
    
    def request_data_dos(self,resourceId,offset):
        
        url = f"https://tablebuilder.singstat.gov.sg/api/table/tabledata/{resourceId}?&offset={offset}"
        get_result = requests.get(url).json()['Data']['row']
        df = pd.json_normalize(get_result,record_path='columns',meta=['rowText','seriesNo','uoM'])
        return df
    
    def get_data_dos(self,df_input):
        
        self._logger.info('DOS' + self._started)
        input_id = df_input[df_input['source']=="DOS"].set_index('dataset')['id'].to_dict()
        
        data_output = {}
        for data_key in tqdm(input_id.keys()):
            resourceId = input_id[data_key]
            df_main = pd.DataFrame()
            not_finished = True; offset = 0
            
            while not_finished:
                df = self.request_data_dos(resourceId,offset)
                df_main = pd.concat([df_main,df],ignore_index=True)
                
                if len(df) <2000:
                    not_finished = False
                else:
                    offset += 2000
            
            data_output.update({data_key:df_main})
        self._logger.info('DOS' + self._completed)
            
        return data_output
    
    def get_domestic_interest_rate_mas(self,df_input):
        
        self._logger.info('MAS interest rate' + self._started)
        input_id = df_input[df_input['source']=="MAS"].set_index('dataset')['id'].to_dict()
        resources_id = input_id['domestic_interest_rate']
        
        start_date = date(1995,1,1)
        end_date = date.today()
        days_count=(end_date-start_date).days
        df_main = pd.DataFrame()
        
        for offset in tqdm(range(0,days_count+100,100)):
            url = f"https://eservices.mas.gov.sg/api/action/datastore/search.json?resource_id={resources_id}&between[end_of_day]={start_date.strftime('%Y-%m')},{end_date.strftime('%Y-%m')}&offset={offset}"
            get_result = requests.get(url).json()['result']['records']
            df = pd.DataFrame.from_records(get_result)
            df_main = pd.concat([df_main,df],ignore_index=True)
        
        self._logger.info('MAS interest rate' + self._completed)
        
        return df_main
    
    def get_neer_mas(self):
        
        self._logger.info('MAS NEER' + self._started)
        # get the url of the file since the url is different each new release
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}
        url_fx = "https://www.mas.gov.sg/statistics/exchange-rates"
        page = requests.get(url_fx,headers=headers)
        tree = html.fromstring(page.content)
        href = tree.xpath('/html/body/div[1]/main/section[2]/div/div/div/div/div/div[2]/p[1]/span/a/@href')[0]
        homepage = "https://www.mas.gov.sg"
        url_neer = homepage + href
        
        get_file = requests.get(url_neer,headers=headers)
        
        with open('neer.xlsx','wb') as file:
            file.write(get_file.content)
        
        self._logger.info('MAS NEER' + self._completed)
        return True


class TransformTool():
    
    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self._started = " data transformation started."
        self._completed = " data transformation completed."
    
    def process_dos(self,key_all):
        
        self._logger.info('DOS' + self._started)
        data_output={}
        for data_key in key_all.keys():
            
            df = key_all[data_key]
            # process quarterly timepoint for standardisation purpose
            if not df[df['key'].str.contains('Q')].empty:
                df['key'] = pd.PeriodIndex(df['key'].str.replace(r'(\d{4}) (\d)Q',r'\1-Q\2'),freq='Q').to_timestamp(how='end')
                df['key'] = pd.to_datetime(df["key"]).dt.date
            else:
                # transform day of month to month end
                df['key'] = pd.to_datetime(df["key"]).dt.date
                df['key'] = (df['key'] + MonthEnd(0)).dt.date
            
            # replace "-" data with nan
            if not df[df['value'].str.contains('^-$',regex=True)].empty:
                df['value'] = df['value'].replace(r'^-$',np.nan, regex=True)
                
            # replace "na" data with nan
            if not df[df['value'].str.contains('na',regex=True,na=False)].empty:
                df['value'] = df['value'].replace(r'na',np.nan, regex=True)
            
            # Concat series number with series name for better identification of series
            df['rowText'] = df['seriesNo'] + ": " + df['rowText']
            
            df['value'] = df['value'].astype(float)
            df = df.pivot_table(values='value',columns='rowText',index=['uoM','key'])
            df.index.names = ['Units','Timepoint']
            
            data_output.update({data_key:df})
            
        self._logger.info('DOS' + self._completed)
        return data_output
    
    def process_mas_domestic_interest_rate(self,df):
        
        self._logger.info('MAS interest rate' + self._started)
        # columns = ['end_of_day','sora','sora_index','highest_transaction',
                   # 'standing_facility_deposit','standing_facility_borrow']
        columns = ['comp_sora_1m','comp_sora_3m','comp_sora_6m','end_of_day','sor_average',
                   'sora','sora_index','standing_facility_borrow','standing_facility_deposit']
        df = df[columns]
        
        df.rename(columns={'end_of_day':'Timepoint','value':'Value'},inplace=True)
        df.set_index(keys='Timepoint',inplace=True)
        df = self.create_units_index(df) #create unit index since the dataset unit data is not provided. This is to facilitate concatenation of all series later
        
        output = {'domestic_interest_rate':df}
        self._logger.info('MAS interest rate' + self._completed)
        return output


    def process_mas_neer(self):
        
        self._logger.info('MAS NEER' + self._started)
        neer_file = pd.read_excel('neer.xlsx',sheet_name=None)
        df_neer = pd.DataFrame()
        
        # process and combine all data from each sheet in the file
        for year in neer_file.keys():

            df = neer_file[year]
            df = df.loc[6:].dropna(axis=0)
            df = df.rename(columns={df.columns[0]:'date',df.columns[1]:'neer'})
            
            # date processing because the date provided is not user-friendly
            df['date'] = df['date'].astype(str)
            df['date'] = df['date'].str.strip()
            df['day'] = df['date'].str.split(' ').str[-1]
            df['date'] = df[df['date'].str.istitle()]['date']
            
            df = df.ffill()
            
            df['month'] = df['date'].str.split(' ').str[:-1].str[-1]
            df['year'] = df['date'].str.split(' ').str[:-2].str[-1]
            df = df.ffill()
            
            df['date'] = df['day'] + "-" + df['month'] + "-" + df['year']
            df['date'] = pd.to_datetime(df['date']).dt.date
            df = df[['date','neer']]
            
            df_neer = pd.concat([df_neer,df],ignore_index=True)
        
        df_neer['neer'] = df_neer['neer'].astype(float)
        df_neer.rename(columns={'date':'Timepoint','neer':'Nominal Effective Exchange Rate','value':'Value'},inplace=True)
        df_neer.set_index(keys='Timepoint',inplace=True)
        df_neer = self.create_units_index(df_neer) #create unit index since the dataset unit data is not provided. This is to facilitate concatenation of all series later

        output = {'neer':df_neer}
        
        self._logger.info('MAS NEER' + self._completed)
        return output
        
    def concat_all(self,bucket_tranform):
        
        self._logger.info('Concat all' + self._started)
        df_all = pd.DataFrame(index=pd.MultiIndex(levels=[[],[]],codes=[[],[]],names=['Units','Timepoint']))
        
        for key in bucket_tranform.keys():
            df = bucket_tranform[key]
            df_all = pd.concat([df_all,df],axis=1,join='outer')
            
        bucket_tranform.update({'all':df_all})
        
        self._logger.info('Concat all' + self._completed)
        return bucket_tranform
    
    def create_units_index(self,df):
        
        current_index = df.index.names[0]
        df['Units'] = np.nan
        df.set_index('Units',append=True,inplace=True)
        df = df.swaplevel('Units',current_index)
        return df
    
class LoadTool():
    
    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self._started = "Data loading started."
        self._completed = "Data loading completed."
    
    def df_to_csv(self,transform_all):
        
        self._logger.info(self._started)
        for dataset in transform_all.keys():
            transform_all[dataset].to_csv(f'database/{dataset}.csv',sep=',',index=True)
    
        self._logger.info(self._completed)
        
        return True