# -*- coding: utf-8 -*-
"""
Created on Sun May  7 14:37:44 2023

@author: chang
"""

import etl_function

extract_tool = etl_function.ExtractTool()
transform_tool = etl_function.TransformTool()
load_tool = etl_function.LoadTool()

def extract(df_input):
    
    extract_dos = extract_tool.get_data_dos(df_input)
    extract_mas_interest_rate = extract_tool.get_domestic_interest_rate_mas(df_input)
    extract_mas_neer = extract_tool.get_neer_mas()
    
    bucket_extract = {'dos':extract_dos,'mas_interest_rate':extract_mas_interest_rate,'mas_neer':extract_mas_neer}
    
    return bucket_extract


def transform(bucket_extract):
    
    transform_dos = transform_tool.process_dos(bucket_extract['dos'])
    transform_mas_interest_rate = transform_tool.process_mas_domestic_interest_rate(bucket_extract['mas_interest_rate'])
    transform_mas_neer = transform_tool.process_mas_neer()
    
    bucket_tranform = transform_dos
    bucket_tranform.update(transform_mas_interest_rate)
    bucket_tranform.update(transform_mas_neer)
    
    # concat all dataset into one dataframe. This will help to facilitate the visualisation in Power BI
    # transform_tool.concat_all(bucket_tranform)
    
    return bucket_tranform

def load(bucket_tranform):
    
    load_tool.df_to_csv(bucket_tranform)
    
    return True