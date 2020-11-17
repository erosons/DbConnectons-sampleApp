import pyodbc
import pandas as pd
import numpy as np
import os
from pathlib import Path
from dbconnect import connect
from pathCreator import pathcreator, rpmpath
import shutil
from script import sqlCommand
from copy_movefile import copy


rpmdata=pd.DataFrame()
df = pd.read_sql_query(sqlCommand, connect(), index_col="CES_LDC_ID",chunksize=10000)
#df.to_csv(index=True, path_or_buf=pathcreator())
for chunk in df:
    rpmdata=pd.concat([rpmdata,chunk])
rpmdata
rpmdata=pd.DataFrame(rpmdata)
del_columns=[]
reqd_cols=["CES_LDC_ID", "LIFECYCLE_STATUS", "SEGMENT", "RATE_PLAN", "CES_RATE_CODE", "USAGE_YRLY_EST",
                     "CONTRACT_START_DT", "CONTRACT_END_DT", "PRICE", "PRICE_UNITS", "UTIL_ACCT_START_DT", "UTIL_ACCT_END_DT", "VENDOR_ID"]
for i in list(rpmdata.columns):
    if i not in reqd_cols:
        del_columns.append(i)

del_columns

rpmdata_filter=rpmdata["BILLING_STATUS"].isin(["ACT"])
rpmdata=rpmdata[rpmdata_filter]
rpmdata=rpmdata.drop(del_columns,axis=1)
rpmdata.to_csv(index=True, path_or_buf=rpmpath())
print("successful")
