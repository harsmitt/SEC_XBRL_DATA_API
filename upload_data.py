#This is to upload data from tsv files into DB using sqlalchemy, pandas

import pandas as pd
import os
from sqlalchemy import create_engine

#enter relevant credentials as per your DB. For Example if your MYSQL server has username=abc and password=xyz and
# hosted on 100.1.1.1 on port 5000 then the syntax will be engine = create_engine("mysql+pymysql://abc:xyz@10.1.1.1:5000/sec_xbrl")

engine = create_engine("mysql+pymysql://harshit:harshit@localhost/sec_xbrl")

for k in os.scandir():
    try:
        if k.name.endswith("tsv"):
            df = pd.read_csv(k, sep= '\t', low_memory = False)
            print("Actual rows in table--",df.shape)
            print("Inserted rows--",df.to_sql(k.name.split(".")[0], engine, index=False, if_exists='append', chunksize=10000))
            print("Uploaded file ", k.name)
    except Exception as e:
        print(e)
        print("Error in ",k.name)
