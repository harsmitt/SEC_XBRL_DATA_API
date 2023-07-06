import json
from datetime import timedelta
import aiomysql
import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import regex
from aiomysql import create_pool
from aiomysql.sa import create_engine as async_engine
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
pd.set_option('display.float_format', '{:,.2f}'.format)
from financials_code_for_DB import *

# MySQL database configuration
MYSQL_HOST = 'localhost'
MYSQL_PORT = 8082
MYSQL_USER = 'harshit'
MYSQL_PASSWORD = 'harshit'
MYSQL_DB = 'sec_xbrl'

# Establish a connection to the MySQL database

async def retrieve_data_from_cache(conn, adsh):
    # Connect to MYSQL
    async with conn.cursor(aiomysql.DictCursor) as cursor:
        await cursor.execute(f"Select value from mongo where adsh_key = '{adsh}'") 
        result = await cursor.fetchall()
        if result:
            data = result[0]['value']
            return json.loads(data)

async def retrieve_data_from_cik(conn, cik, period_type):
    # Connect to MYSQL
    async with conn.cursor(aiomysql.DictCursor) as cursor:
        if period_type == "annual":
            await cursor.execute(f"Select * from sub where cik = {cik} and form in ('10-K', '20-F')") 
            result = await cursor.fetchall()
        else:
            await cursor.execute(f"Select * from sub where cik = {cik} and form in ('10-Q', '6-K')") 
            result = await cursor.fetchall()
        if result:
            return result

async def retrieve_data_from_adsh_ren(conn,adsh):
    # Connect to MYSQL
    async with conn.cursor(aiomysql.DictCursor) as cursor:
        await cursor.execute(f"Select * from ren where adsh = '{adsh}' and menucat = 's'") 
        result = await cursor.fetchall()
        if result:
            return result

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Fetch data from the MySQL database
async def fetch_filing(conn, adsh):
    async with conn.cursor(aiomysql.DictCursor) as cursor:
        await cursor.execute(f"SELECT * FROM sub WHERE adsh = '{adsh}'")
        result = await cursor.fetchall()
        return result

# Fetch data from the MySQL database
async def fetch_ren(conn, adsh):
    async with conn.cursor(aiomysql.DictCursor) as cursor:
        await cursor.execute(f"SELECT * FROM ren WHERE adsh = '{adsh}' and menucat = 's';")
        result = await cursor.fetchall()
        return result

# Define a FastAPI endpoint to retrieve user data
@app.get("/filing/{adsh}")
async def get_filing_data(adsh: str):
    conn = await connect_to_mysql()
    cached_data = await retrieve_data_from_cache(conn, "%s_filing"%adsh)
    # First, check if the data is already in the Redis cache
    if cached_data:
        return {"data": cached_data, "source": "MYSQL-JSON"}
        
    # If not, fetch the data from the MySQL database
    fetched_data = await fetch_filing(conn, adsh)
    fetched_data = json.loads(json.dumps(fetched_data,default=str))

    # Store the fetched data in the Redis cache
    await save_data_to_mysql_json(conn, fetched_data, "%s_filing"%adsh)

    return {"data": fetched_data, "source": "mysql"}

@app.get("/render/{adsh}")
async def get_render_data(adsh: str):
    conn = await connect_to_mysql()
    cached_data = await retrieve_data_from_cache(conn, "%s_render"%adsh)
    # First, check if the data is already in the Redis cache

    if cached_data:
        return {"data": cached_data, "source": "MYSQL-JSON"}
        

    # If not, fetch the data from the MySQL database
    fetched_data = await fetch_ren(conn, adsh)

    # Store the fetched data in the Redis cache
     
    await save_data_to_mysql_json(conn, json.loads(json.dumps(fetched_data,default=str)), "%s_render"%adsh)

    return {"data": fetched_data, "source": "mysql"}

@app.get("/financials/{adsh}")
async def get_financials(adsh):
    
    conn = await connect_to_mysql()

    filing_data = await retrieve_data_from_adsh(conn,adsh)
    
    period = filing_data[0]['period']
    file_name = filing_data[0]['file_name']
    fp = filing_data[0]['fp']
    try:
        qtr = int(regex.search('\d',fp).group())
    except:
        qtr = 4
    cdate = period
    pdate = cdate - relativedelta(months=12, days=5) + relativedelta(day=35)
    pdate_bs = cdate - relativedelta(months=3*qtr) - timedelta(days=5) + relativedelta(day=35)

    cached_data = await retrieve_data_from_cache(conn, "%s_fin_%s"%(adsh,cdate))

    if cached_data:
        return {"data": cached_data, "source": "MYSQL-JSON"}
   
    all_stmts = await afin_stmts(adsh, file_name, period, fp, qtr, cdate, pdate_bs)
    fetched_data =  json.loads(all_stmts.to_json(orient = 'records'))
    
    # Store the fetched data in the MYSQL JSON table
    await save_data_to_mysql_json(conn, fetched_data, "%s_fin_%s"%(adsh,cdate))
    
    return {"data": fetched_data, "source": "mysql"}

@app.get("/financials_for_2periods/{adsh}")
async def get_financials_2_periods(adsh):
    
    conn = await connect_to_mysql()

    filing_data = await retrieve_data_from_adsh(conn,adsh)
    
    period = filing_data[0]['period']
    file_name = filing_data[0]['file_name']
    fp = filing_data[0]['fp']
    try:
        qtr = int(regex.search('\d',fp).group())
    except:
        qtr = 4
    cdate = period
    pdate = cdate - relativedelta(months=12, days=5) + relativedelta(day=35)
    pdate_bs = cdate - relativedelta(months=3*qtr) - timedelta(days=5) + relativedelta(day=35)
    pdate_bs2 = pdate_bs - relativedelta(months=12) - timedelta(days=5) + relativedelta(day=35)


    cached_data1 = await retrieve_data_from_cache(conn, "%s_fin_%s"%(adsh,cdate))
    cached_data2 = await retrieve_data_from_cache(conn, "%s_fin_%s"%(adsh,pdate))

    if cached_data1 and cached_data2:
        try:
            df1 = pd.DataFrame(cached_data1)
            df2 = pd.DataFrame(cached_data2)
            df2.drop(['uom', 'datp', 'dcml', 'durp', 'coreg','footlen','footnote'],axis=1,inplace=True)
            df_final = pd.merge(df1, df2, on=['adsh', 'file_name', 'inpth', 'line', 'negating', 'plabel', 'prole','report', 'stmt', 'tag',
                                               'version', 'dimh', 'dimn', 'iprx', 'qtrs'], how='left')
            main_df = json.loads(df_final.to_json(orient = 'records'))
            return {"data": main_df, "source": "MYSQL-JSON"}
        except Exception as e:
            print("Error occured: ",e)
            pass
            return {"data":None} 
    
    all_stmts = await afin_stmts_2periods(adsh, file_name, period, fp, qtr, cdate, pdate, pdate_bs, pdate_bs2)
    if len(all_stmts) == 2:
        try:                
            fetched_data1 =  json.loads(all_stmts[0].to_json(orient = 'records'))
            fetched_data2 =  json.loads(all_stmts[1].to_json(orient = 'records'))
            try:
                await save_data_to_mysql_json(conn, fetched_data1, "%s_fin_%s"%(adsh,cdate))
            except:
                pass
            try:
                await save_data_to_mysql_json(conn, fetched_data2, "%s_fin_%s"%(adsh,pdate))
            except:
                pass
            all_stmts[1].drop(['uom', 'datp', 'dcml', 'durp', 'coreg','footlen','footnote'],axis=1,inplace=True)
            df_final = pd.merge(all_stmts[0], all_stmts[1], on=['adsh', 'file_name', 'inpth', 'line', 'negating', 'plabel', 'prole','report',
                                                                 'stmt', 'tag', 'version', 'dimh', 'dimn', 'iprx', 'qtrs'], how='left')
            main_df = json.loads(df_final.to_json(orient = 'records'))    
            return {"data": main_df, "source": "MYSQL"}
        except Exception as e:
            print("Error occured level2: ",e)
            pass
            return {"data":None} 

@app.get("/financials_for_2periods_1stmt/{stmt}/{adsh}")
async def get_financials_2_periods_1stmt(adsh,stmt):
    
    conn = await connect_to_mysql()

    filing_data = await retrieve_data_from_adsh(conn,adsh)
    
    period = filing_data[0]['period']
    file_name = filing_data[0]['file_name']
    fp = filing_data[0]['fp']
    try:
        qtr = int(regex.search('\d',fp).group())
    except:
        qtr = 4
    cdate = period
    pdate = cdate - relativedelta(months=12, days=5) + relativedelta(day=35)
    pdate_bs = cdate - relativedelta(months=3*qtr) - timedelta(days=5) + relativedelta(day=35)
    pdate_bs2 = pdate_bs - relativedelta(months=12) - timedelta(days=5) + relativedelta(day=35)

    cached_data1 = await retrieve_data_from_cache(conn, "%s_fin_%s"%(adsh,cdate))
    cached_data2 = await retrieve_data_from_cache(conn, "%s_fin_%s"%(adsh,pdate))

    if cached_data1 and cached_data2:
        try:
            df1 = pd.DataFrame(cached_data1)
            df2 = pd.DataFrame(cached_data2)
            df2.drop(['uom', 'datp', 'dcml', 'durp', 'coreg','footlen','footnote'],axis=1,inplace=True)
            df_final = pd.merge(df1, df2, on=['adsh', 'file_name', 'inpth', 'line', 'negating', 'plabel', 'prole','report', 'stmt', 'tag',
                                               'version', 'dimh', 'dimn', 'iprx', 'qtrs'], how='left')
            if stmt == "pnl":
                df_final = df_final.query("stmt=='IS'")
            elif stmt == "bs":
                df_final = df_final.query("stmt=='BS'")
            elif stmt == "cf":
                df_final = df_final.query("stmt=='CF'")
            main_df = json.loads(df_final.to_json(orient = 'records'))
            return {"data": main_df, "source": "MYSQL-JSON"}
        except Exception as e:
            print("Error occured: ",e)
            pass
            return {"data":None} 
    
    all_stmts = await afin_stmts_2periods(adsh, file_name, period, fp, qtr, cdate, pdate, pdate_bs, pdate_bs2)
            
    if len(all_stmts) == 2:
        try:                            
            fetched_data1 =  json.loads(all_stmts[0].to_json(orient = 'records'))
            fetched_data2 =  json.loads(all_stmts[1].to_json(orient = 'records'))
            try:
                await save_data_to_mysql_json(conn, fetched_data1, "%s_fin_%s"%(adsh,cdate))
            except:
                pass
            try:
                await save_data_to_mysql_json(conn, fetched_data2, "%s_fin_%s"%(adsh,pdate))
            except:
                pass
            all_stmts[1].drop(['uom', 'datp', 'dcml', 'durp', 'coreg','footlen','footnote'],axis=1,inplace=True)
            df_final = pd.merge(all_stmts[0], all_stmts[1], on=['adsh', 'file_name', 'inpth', 'line', 'negating', 'plabel', 'prole','report',
                                                                 'stmt', 'tag', 'version', 'dimh', 'dimn', 'iprx', 'qtrs'], how='left')    
            if stmt == "pnl":
                df_final = df_final.query("stmt=='IS'")
            elif stmt == "bs":
                df_final = df_final.query("stmt=='BS'")
            elif stmt == "cf":
                df_final = df_final.query("stmt=='CF'")
            main_df = json.loads(df_final.to_json(orient = 'records'))    
            return {"data": main_df, "source": "MYSQL"}
        except Exception as e:
            print("Error occured level2: ",e)
            pass
            return {"data":None} 

@app.get("/annual_financials_for_multi_periods/{cik}")
async def get_annual_financials_multi_periods(cik:int, count:int = 2):
    
    conn = await connect_to_mysql()

    cik_data = await retrieve_data_from_cik(conn,cik,"annual")
    cik_df = pd.DataFrame(cik_data)
    cik_df['period'] = pd.to_datetime(cik_df['period'])
    cik_df.sort_values('period',ascending=False, inplace=True)

    queries = []
    cdate = cik_df.iloc[0]['period'].date()
    pdate = cdate - relativedelta(months=12) + relativedelta(day=35)
    adsh = cik_df.iloc[0]['adsh']
    queries.append(f"select value from mongo where adsh_key = '{adsh}_fin_{cdate}'")
    queries.append(f"select value from mongo where adsh_key = '{adsh}_fin_{pdate}'")

    for i,j in cik_df.iloc[1:].iterrows():
        cdate = j['period'].date()
        pdate = cdate - relativedelta(months=12) + relativedelta(day=35)
        adsh = j['adsh']
        queries.append(f"select value from mongo where adsh_key = '{adsh}_fin_{pdate}'")
    
    results = await run_queries(queries[:count])
    results = [pd.DataFrame(json.loads(i.loc[0,'value'])) for i in results]

    if count == 1:
        merged_df = results[0]

    elif count == 2:
        for i in results:
            i.drop(['coreg','footlen','footnote','datp'],axis=1,inplace=True)
            i.dropna(subset='value',inplace=True)
            date = pd.to_datetime(i.iloc[0]['ddate'],unit='ms').date()
            i.rename(columns={'ddate':'date_%s'%date,'value':'data_%s'%date,'value2':'data2_%s'%date},inplace=True)
            
        merged_df = pd.merge(results[0], results[1], on=['adsh', 'file_name', 'inpth', 'line', 'negating', 'plabel', 'prole','report', 'stmt',
                                                    'tag', 'version','dcml','dimh', 'dimn', 'durp', 'iprx', 'qtrs','uom'], how='left')
    elif 20 >= count > 2:
        merged_df = arrange_stmt(results)

    else:
        return "Count needs to be a positive integer between 1 and 20"
    
    main_df = json.loads(merged_df.to_json(orient = 'records'))
    return {"data": main_df, "source": "MYSQL-JSON"}

@app.get("/annual_financials_for_multi_periods2/{cik}")
async def get_annual_financials_multi_periods2(cik:int, count:int = 2):
    if count <= 0:
        return "Count needs to be a positive integer between 1 and 20"
    else:
        conn = await connect_to_mysql()

        cik_data = await retrieve_data_from_cik(conn,cik,"annual")
        cik_df = pd.DataFrame(cik_data)
        cik_df['period'] = pd.to_datetime(cik_df['period'])
        cik_df.sort_values('period',ascending=False, inplace=True)

        queries = []
        cdate = cik_df.iloc[0]['period'].date()
        pdate = cdate - relativedelta(months=12) + relativedelta(day=35)
        adsh = cik_df.iloc[0]['adsh']
        queries.append(f"select value from mongo where adsh_key = '{adsh}_fin_{cdate}'")
        queries.append(f"select value from mongo where adsh_key = '{adsh}_fin_{pdate}'")
        queries.append(f"select value from mongo where adsh_key = '{adsh}_fin_2015-12-31'")

        for i,j in cik_df.iloc[1:].iterrows():
            cdate = j['period'].date()
            pdate = cdate - relativedelta(months=12) + relativedelta(day=35)
            adsh = j['adsh']
            queries.append(f"select value from mongo where adsh_key = '{adsh}_fin_{pdate}'")
        
        results = await run_queries(queries[:count])

        adsh_list = []
        for i,j in enumerate(results):
            if j.empty:
                adsh_list.append(queries[i][42:62])

        for adsh in adsh_list:
            await afin_stmts_2periods_4mongo(adsh)

        results = await run_queries(queries[:count])
        results = [pd.DataFrame(json.loads(i.loc[0,'value'])) for i in results]

        if count == 1:
            merged_df = results[0]

        elif count == 2:
            for i in results:
                i.drop(['coreg','footlen','footnote','datp'],axis=1,inplace=True)
                i.dropna(subset='value',inplace=True)
                date = pd.to_datetime(i.iloc[0]['ddate'],unit='ms').date()
                i.rename(columns={'ddate':'date_%s'%date,'value':'data_%s'%date,'value2':'data2_%s'%date},inplace=True)
                
            merged_df = pd.merge(results[0], results[1], on=['adsh', 'file_name', 'inpth', 'line', 'negating', 'plabel', 'prole','report', 'stmt',
                                                        'tag', 'version','dcml','dimh', 'dimn', 'durp', 'iprx', 'qtrs','uom'], how='left')
        elif 20 >= count > 2:
            merged_df = arrange_stmt(results)

        else:
            return "Count needs to be a positive integer between 1 and 20"
        
        main_df = json.loads(merged_df.to_json(orient = 'records'))
        return {"data": main_df, "source": "MYSQL-JSON"}



    



    

