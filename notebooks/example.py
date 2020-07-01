#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May  8 10:02:57 2020

@author: gjacopo
"""

x=pandas.read_csv('/Users/gjacopo/Downloads/aact_ali01.tsv',sep='\t',encoding='latin1')

import numpy as np#analysis:ignore
import pandas as pd#analysis:ignore

from pyeudatnat.base import datnatFactory
from pyeudatnat.misc import Structure
from pyeudatnat.misc import Type, Datetime
    
def prepare(data):
    first_col = data.columns[0]
    cols = first_col.split('\\')[0].split(',')
    def split_column(col):
        return col.split(',')
    data[cols] = data.apply(lambda row: pd.Series(split_column(row[first_col])), axis=1)
    return cols
                                                                       
def prepare_data(self):
    cols = prepare(self.data)
    # add the columns as inputs (they were created)
    self.columns.extend([{self.lang:col for col in cols}])
    # add the data as outputs (they will be stored)
    self.index.update({col:col for col in cols})

EUdata = datnatFactory(country = None)
