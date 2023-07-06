# SEC_XBRL_DATA_API

This is a project in which i have created an API hosted on own/AWS server to get data from any SEC financial filing based on its unique key. For this, i have downloaded all the files from SEC URL: https://www.sec.gov/about/divisions-offices/division-economic-risk-analysis/data/financial-statement-and-notes-data-set

The United States (US) SEC provides all the XBRL filings in a tsv format in zip files for each month after 3-4 days. We can download this archive and see any US listed company's financial data from the filing. There are 8 tables in each zip file which needs to be uploaded in a MYSQL DB. To do so, i have created a py file which uploads data from the folder in their respective table. 

API endpoints return json format data which can be used in any program.

The stack used aiomysql, pandas, Fastapi, sqlalchemy, numpy, asyncio.

Technical note: The query takes time to aggregate result hence i have created a table named mongo in MYSQL using json format. This acts as a cache for the data which has been viewed once, if the filing has been redered once to the API then the data will be stored in mongo table and thereafter all the calls will be fetching data from the cache to speed up the response. Anyone can change this to redis, MongoDB to further speed up the calls by doing some modifications if desired.  

Steps:
1. First create a MYSQL server and then using the sec_xbrl.sql file import it into the server. This will create necessary tables.
2. Extract the files from zip file one by one into a folder and in that folder run the script upload_data.py to upload the data from file to MYSQL table.
3. After the data is uploaded, run commmand "uvicorn app:app" this will start a localhost server which will fetch data by running queries on the DB tables.
4. There are mainly 3 URLS which can be used to check the data:

EX: localhost:8000/filing/{adsh}    ---    localhost:8000/filing/0000320193-22-000108   ----   this will give you filing info.
EX: localhost:8000/financials/{adsh}    ---    localhost:8000/financials/0000320193-22-000108   ----   this will give financials for latest year from the filing
EX: localhost:8000/financials_for_2periods/{adsh}    ---    localhost:8000/financials_for_2periods/0000320193-22-000108   ----   this will give financials for two years latest from the filing

