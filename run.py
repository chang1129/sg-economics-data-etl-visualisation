# -*- coding: utf-8 -*-
"""
Created on Sun May  7 14:34:17 2023

@author: chang
"""

import etl_job
import pandas as pd

df_input = pd.read_excel("input.xlsx")

def etl_report(df_input):

    extract_all = etl_job.extract(df_input)
    transfrom_all = etl_job.transform(extract_all)
    etl_job.load(transfrom_all)
    return True

if __name__=="__main__":
    etl_report(df_input)