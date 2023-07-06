
import pandas as pd
from datetime import timedelta
pd.set_option('display.float_format', '{:,.2f}'.format)
from sqlalchemy import create_engine
import hashlib
import numpy as np
import regex, json
from dateutil.relativedelta import relativedelta
import asyncio
import aiomysql
from aiomysql.sa import create_engine as async_engine
from aiomysql import create_pool


config = {'user': 'harshit', 'password': 'harshit', 'host': 'localhost', 'port':8082, 'db':'sec_xbrl'}
engine = create_engine("mysql+pymysql://harshit:harshit@localhost:8082/sec_xbrl")

# MySQL database configuration
MYSQL_HOST = 'localhost'
MYSQL_PORT = 8082
MYSQL_USER = 'harshit'
MYSQL_PASSWORD = 'harshit'
MYSQL_DB = 'sec_xbrl'

async def connect_to_mysql():
    return await aiomysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        db=MYSQL_DB
    )

async def retrieve_data_from_adsh(conn, adsh):
    # Connect to MYSQL
    async with conn.cursor(aiomysql.DictCursor) as cursor:
        await cursor.execute(f"Select * from sub where adsh = '{adsh}'") 
        result = await cursor.fetchall()
        if result:
            return result

async def save_data_to_mysql_json(conn, data, adsh):
    # Connect to MYSQL
    async with conn.cursor(aiomysql.DictCursor) as cursor:
        await cursor.execute("Insert into mongo (adsh_key, value) values (%s, %s)", (adsh,json.dumps(data),)) 
        await conn.commit()

def dcml_apply(row):
    if row['dcml'] != np.nan and row['value'] != np.nan:
        if -10.0 < row['dcml'] < 0.0 :
            return row['value'] * (10**row['dcml'])
        elif 0.0 < row['dcml'] < 25.0 :
            return np.round(row['value'], int(row['dcml']))
        else:
            return row['value']

#Main function to merge financials
def create_stmt_table(stmt_q, num, qtr, fp):

    stmt_table = pd.merge(stmt_q, num, how="left", left_on=['tag','version'], right_on=['tag','version'])

    if stmt_table.loc[0,'stmt'] == "BS":
        stmt_table = stmt_table[(stmt_table['qtrs'].isna()) | (stmt_table['qtrs']==0)]
    elif stmt_table.loc[0,'stmt'] == "CF":
        stmt_table = stmt_table[(stmt_table['qtrs'].isna()) | (stmt_table['qtrs']==qtr) | (stmt_table['qtrs']==0)]
    elif stmt_table.loc[0,'stmt'] == "IS" and "Q" in fp.upper():
        stmt_table = stmt_table[(stmt_table['qtrs'].isna()) | (stmt_table['qtrs']==1) | (stmt_table['qtrs']==0)]
    elif stmt_table.loc[0,'stmt'] == "IS" and "FY" in fp.upper():
        stmt_table = stmt_table[(stmt_table['qtrs'].isna()) | (stmt_table['qtrs']==qtr) | (stmt_table['qtrs']==0)]


    dims = {}
    for i in stmt_q[stmt_q['tag'].str.endswith("Axis")]['tag']:
        for j in stmt_q[stmt_q['tag'].str.endswith("Member")]['tag']:
            dim_text = i.rsplit("Axis",1)[0] + "=" + j.rsplit("Member",1)[0] + ";"
            dimh = "0x" + hashlib.md5(dim_text.encode()).hexdigest()
            obj = {dimh : j}
            dims.update(obj)

    for i,j in stmt_table['dimh'].items():
        if j not in [i for i in dims.keys()] + [np.nan , '0x00000000']:
            stmt_table.drop(i,inplace=True)

    stmt_table.sort_values(['line','dimn'],ascending=[True,False], inplace=True)

    for i,j in stmt_table[['tag','version','dimh','prole','plabel']].iterrows():
        if "total" not in j['prole'].lower(): 
            if j['dimh'] in dims and ("gaap" in j['version'].lower() or "ifrs" in j['version'].lower()):
                new_label = j['plabel'] +" - "+ stmt_table[stmt_table['tag']=="%s"%dims[j['dimh']]]['plabel'].values[0]
                if "[" in new_label:
                    new_label = new_label.split("[")[0].strip()
                    stmt_table.loc[i,'plabel'] = new_label
                else:
                    stmt_table.loc[i,'plabel'] = new_label

    if True in stmt_table['tag'].duplicated().unique():
        for i, j in stmt_table[stmt_table['tag'].duplicated(False)][['prole','plabel','dimn']].iterrows():
            if j['dimn'] == 0 and "total" not in j['plabel'].lower():
                stmt_table.loc[i,"plabel"] = "Total " + j['plabel']

    stmt_table['value2'] = stmt_table.apply(lambda row: dcml_apply(row), axis=1)
    stmt_table['value2'] = stmt_table.apply(lambda x: x['value2'] * -1 if x['negating']==1 else x['value2'], axis=1)

    return stmt_table

# create a coroutine to execute a query
async def execute_query(query):
#     create a connection to the MySQL server
    pool = await async_engine(
        minsize=6,
        host='103.98.21.4',
        port = 8082,
        # host='10.10.0.141',
        # port = 3306,
        user='harshit',
        password='harshit',
        db='sec_xbrl',
    )

    # create a cursor object to execute the query
    async with pool.acquire() as cursor:
        result = await cursor.execute(query)
        data = await result.fetchall()
        df = pd.DataFrame(data)
        return df

# run the queries concurrently
async def run_queries(queries):
    tasks = [asyncio.create_task(execute_query(query)) for query in queries]
    results = await asyncio.gather(*tasks)
    return results

# results = await run_queries()


async def afin_stmts(adsh, file_name, period, fp, qtr, cdate, pdate_bs):
    try:
        queries = ["SELECT * FROM pre WHERE file_name = '%s' and adsh = '%s' and stmt in ('IS','BS','CF') ORDER by line asc;"%(file_name, adsh),
        "SELECT tag, version, ddate, qtrs, uom, dimh, iprx, value, footnote, footlen, dimn, coreg, durp, datp, dcml FROM num where file_name = '%s' and adsh = '%s' and ddate = '%s' and iprx = 0;"%(file_name, adsh, cdate),
        "SELECT tag, version, ddate, qtrs, uom, dimh, iprx, value, footnote, footlen, dimn, coreg, durp, datp, dcml FROM num where file_name = '{0}' and adsh = '{1}' and ddate = '{2}' and tag REGEXP 'Cash' and iprx = 0 and dimn = 0;".format(file_name, adsh, pdate_bs)
        ]
        
        results = await run_queries(queries)
        pre_q, num, cash = results
        pl_rpt = np.min(pre_q.query('stmt=="IS"')['report'].unique())
        bs_rpt = np.min(pre_q.query('stmt=="BS"')['report'].unique())
        cf_rpt = np.min(pre_q.query('stmt=="CF"')['report'].unique())
        pnl_q, bs_q, cf_q = pre_q.query(f'stmt=="IS" and report == {pl_rpt}') , pre_q.query(f'stmt=="BS" and report == {bs_rpt}') , pre_q.query(f'stmt=="CF" and report == {cf_rpt}')
        

        pnl_table = create_stmt_table(pnl_q, num, qtr, fp)
        bs_table = create_stmt_table(bs_q, num, qtr, fp)
        cf_table = create_stmt_table(cf_q, num, qtr, fp)

        if True in cf_table['prole'].str.contains("Start").unique():

            cash_value = cash[(cash['tag'] == cf_table.loc[cf_table['prole'].str.contains("Start"),'tag'].values[0]) & (cash['version'] == cf_table.loc[cf_table['prole'].str.contains("Start"),'version'].values[0])]
            cf_table.loc[cf_table['prole'].str.contains("Start"),['ddate','qtrs','uom','dimh','iprx','value','footnote','footlen','dimn','coreg','durp','datp','dcml']] = cash_value[['ddate','qtrs','uom','dimh','iprx','value','footnote','footlen','dimn','coreg','durp','datp','dcml']].values
            cf_table['value2'] = cf_table.apply(lambda row: dcml_apply(row), axis=1)
            cf_table['value2'] = cf_table.apply(lambda x: x['value2'] * -1 if x['negating']==1 else x['value2'], axis=1)

        results = pd.concat([pnl_table, bs_table, cf_table])
        return results
    
    except Exception as e:
        print(e)
        pass

async def afin_stmts_2periods(adsh, file_name, period, fp, qtr, cdate, pdate, pdate_bs, pdate_bs2):
    try:
        queries = [
        "SELECT * FROM pre WHERE file_name = '%s' and adsh = '%s' and stmt in ('IS','BS','CF') ORDER by line asc;"%(file_name, adsh),
        "SELECT tag, version, ddate, qtrs, uom, dimh, iprx, value, footnote, footlen, dimn, coreg, durp, datp, dcml FROM num where file_name = '%s' and adsh = '%s' and ddate in ('%s','%s','%s') and iprx = 0;"%(file_name, adsh, cdate, pdate, pdate_bs),
        "SELECT tag, version, ddate, qtrs, uom, dimh, iprx, value, footnote, footlen, dimn, coreg, durp, datp, dcml FROM num where file_name = '{0}' and adsh = '{1}' and ddate in ('{2}','{3}') and tag REGEXP 'Cash' and iprx = 0 and dimn = 0;".format(file_name, adsh, pdate_bs, pdate_bs2)
        ]

        results = await run_queries(queries)
        pre_q, num_t, cash_t = results
        pl_rpt = np.min(pre_q.query('stmt=="IS"')['report'].unique())
        bs_rpt = np.min(pre_q.query('stmt=="BS"')['report'].unique())
        cf_rpt = np.min(pre_q.query('stmt=="CF"')['report'].unique())
        pnl_q, bs_q, cf_q = pre_q.query(f'stmt=="IS" and report == {pl_rpt}') , pre_q.query(f'stmt=="BS" and report == {bs_rpt}') , pre_q.query(f'stmt=="CF" and report == {cf_rpt}')
        num_1 = num_t[num_t['ddate']==cdate]
        num_2 = num_t[num_t['ddate']==pdate]
        num_bs_prev = num_t[num_t['ddate']==pdate_bs]
        cash = cash_t[cash_t['ddate']==pdate_bs]
        cash2 = cash_t[cash_t['ddate']==pdate_bs2]

        pnl_table = create_stmt_table(pnl_q, num_1, qtr, fp)
        bs_table = create_stmt_table(bs_q, num_1, qtr, fp)
        cf_table = create_stmt_table(cf_q, num_1, qtr, fp)

        if True in cf_table['prole'].str.contains("Start").unique():
            cash_value = cash[(cash['tag'] == cf_table.loc[cf_table['prole'].str.contains("Start"),'tag'].values[0]) & (cash['version'] == cf_table.loc[cf_table['prole'].str.contains("Start"),'version'].values[0])]
            cf_table.loc[cf_table['prole'].str.contains("Start"),['ddate','qtrs','uom','dimh','iprx','value','footnote','footlen','dimn','coreg','durp','datp','dcml']] = cash_value[['ddate','qtrs','uom','dimh','iprx','value','footnote','footlen','dimn','coreg','durp','datp','dcml']].values
            cf_table['value2'] = cf_table.apply(lambda row: dcml_apply(row), axis=1)
            cf_table['value2'] = cf_table.apply(lambda x: x['value2'] * -1 if x['negating']==1 else x['value2'], axis=1)

        results = pd.concat([pnl_table, bs_table, cf_table])

        try:
            pnl_table2 = create_stmt_table(pnl_q, num_2, qtr, fp)
            bs_table2 = create_stmt_table(bs_q, num_bs_prev, qtr, fp)
            cf_table2 = create_stmt_table(cf_q, num_2, qtr, fp)

            if True in cf_table2['prole'].str.contains("Start").unique():
                cash_value2 = cash2[(cash2['tag'] == cf_table2.loc[cf_table2['prole'].str.contains("Start"),'tag'].values[0]) & (cash2['version'] == cf_table2.loc[cf_table2['prole'].str.contains("Start"),'version'].values[0])]
                cf_table2.loc[cf_table2['prole'].str.contains("Start"),['ddate','qtrs','uom','dimh','iprx','value','footnote','footlen','dimn','coreg','durp','datp','dcml']] = cash_value2[['ddate','qtrs','uom','dimh','iprx','value','footnote','footlen','dimn','coreg','durp','datp','dcml']].values
                cf_table2['value2'] = cf_table2.apply(lambda row: dcml_apply(row), axis=1)
                cf_table2['value2'] = cf_table2.apply(lambda x: x['value2'] * -1 if x['negating']==1 else x['value2'], axis=1)

            results2 = pd.concat([pnl_table2, bs_table2, cf_table2])

        except Exception as e:
            print("Previous period is having some issue:",e)
            pass

        if 'results2' in locals() and results2.empty != True:
            return [results,results2]
        else:
            return [results]
        
    except Exception as e:
        print("Some error occured:", e)
        pass

async def afin_stmts_2periods_4mongo(adsh):
        
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

    try:
        queries = [
        "SELECT * FROM pre WHERE file_name = '%s' and adsh = '%s' and stmt in ('IS','BS','CF') ORDER by line asc;"%(file_name, adsh),
        "SELECT tag, version, ddate, qtrs, uom, dimh, iprx, value, footnote, footlen, dimn, coreg, durp, datp, dcml FROM num where file_name = '%s' and adsh = '%s' and ddate in ('%s','%s','%s') and iprx = 0;"%(file_name, adsh, cdate, pdate, pdate_bs),
        "SELECT tag, version, ddate, qtrs, uom, dimh, iprx, value, footnote, footlen, dimn, coreg, durp, datp, dcml FROM num where file_name = '{0}' and adsh = '{1}' and ddate in ('{2}','{3}') and tag REGEXP 'Cash' and iprx = 0 and dimn = 0;".format(file_name, adsh, pdate_bs, pdate_bs2)
        ]

        results = await run_queries(queries)
        pre_q, num_t, cash_t = results
        pl_rpt = np.min(pre_q.query('stmt=="IS"')['report'].unique())
        bs_rpt = np.min(pre_q.query('stmt=="BS"')['report'].unique())
        cf_rpt = np.min(pre_q.query('stmt=="CF"')['report'].unique())
        pnl_q, bs_q, cf_q = pre_q.query(f'stmt=="IS" and report == {pl_rpt}') , pre_q.query(f'stmt=="BS" and report == {bs_rpt}') , pre_q.query(f'stmt=="CF" and report == {cf_rpt}')
        num_1 = num_t[num_t['ddate']==cdate]
        num_2 = num_t[num_t['ddate']==pdate]
        num_bs_prev = num_t[num_t['ddate']==pdate_bs]
        cash = cash_t[cash_t['ddate']==pdate_bs]
        cash2 = cash_t[cash_t['ddate']==pdate_bs2]

        pnl_table = create_stmt_table(pnl_q, num_1, qtr, fp)
        bs_table = create_stmt_table(bs_q, num_1, qtr, fp)
        cf_table = create_stmt_table(cf_q, num_1, qtr, fp)

        if True in cf_table['prole'].str.contains("Start").unique():
            cash_value = cash[(cash['tag'] == cf_table.loc[cf_table['prole'].str.contains("Start"),'tag'].values[0]) & (cash['version'] == cf_table.loc[cf_table['prole'].str.contains("Start"),'version'].values[0])]
            cf_table.loc[cf_table['prole'].str.contains("Start"),['ddate','qtrs','uom','dimh','iprx','value','footnote','footlen','dimn','coreg','durp','datp','dcml']] = cash_value[['ddate','qtrs','uom','dimh','iprx','value','footnote','footlen','dimn','coreg','durp','datp','dcml']].values
            cf_table['value2'] = cf_table.apply(lambda row: dcml_apply(row), axis=1)
            cf_table['value2'] = cf_table.apply(lambda x: x['value2'] * -1 if x['negating']==1 else x['value2'], axis=1)

        results = pd.concat([pnl_table, bs_table, cf_table])

        try:
            pnl_table2 = create_stmt_table(pnl_q, num_2, qtr, fp)
            bs_table2 = create_stmt_table(bs_q, num_bs_prev, qtr, fp)
            cf_table2 = create_stmt_table(cf_q, num_2, qtr, fp)

            if True in cf_table2['prole'].str.contains("Start").unique():
                cash_value2 = cash2[(cash2['tag'] == cf_table2.loc[cf_table2['prole'].str.contains("Start"),'tag'].values[0]) & (cash2['version'] == cf_table2.loc[cf_table2['prole'].str.contains("Start"),'version'].values[0])]
                cf_table2.loc[cf_table2['prole'].str.contains("Start"),['ddate','qtrs','uom','dimh','iprx','value','footnote','footlen','dimn','coreg','durp','datp','dcml']] = cash_value2[['ddate','qtrs','uom','dimh','iprx','value','footnote','footlen','dimn','coreg','durp','datp','dcml']].values
                cf_table2['value2'] = cf_table2.apply(lambda row: dcml_apply(row), axis=1)
                cf_table2['value2'] = cf_table2.apply(lambda x: x['value2'] * -1 if x['negating']==1 else x['value2'], axis=1)

            results2 = pd.concat([pnl_table2, bs_table2, cf_table2])

        except Exception as e:
            print("Previous period is having some issue:",e)
            pass

        if 'results2' in locals() and results2.empty != True:                 
            fetched_data1 =  json.loads(results.to_json(orient = 'records'))
            fetched_data2 =  json.loads(results2.to_json(orient = 'records'))
            try:
                await save_data_to_mysql_json(conn, fetched_data1, "%s_fin_%s"%(adsh,cdate))
            except:
                pass
            try:
                await save_data_to_mysql_json(conn, fetched_data2, "%s_fin_%s"%(adsh,pdate))
            except:
                pass
        else:
            fetched_data1 =  json.loads(results.to_json(orient = 'records'))
            try:
                await save_data_to_mysql_json(conn, fetched_data1, "%s_fin_%s"%(adsh,cdate))
            except:
                pass
    except Exception as e:
        print("Some error occured:", e)
        pass

async def afin_stmts_2periods_pnl(adsh, file_name, period, fp, qtr, cdate, pdate, pdate_bs, pdate_bs2):
    try:
        queries = ["SELECT * FROM pre WHERE file_name = '%s' and adsh = '%s' and stmt = 'IS' ORDER by line asc;"%(file_name, adsh),
        "SELECT tag, version, ddate, qtrs, uom, dimh, iprx, value, footnote, footlen, dimn, coreg, durp, datp, dcml FROM num where file_name = '%s' and adsh = '%s' and ddate in ('%s','%s') and iprx = 0;"%(file_name, adsh, cdate, pdate),
        ]

        results = await run_queries(queries)
        pre_q, num_t = results
        pl_rpt = np.min(pre_q.query('stmt=="IS"')['report'].unique())
        pnl_q = pre_q.query(f'stmt=="IS" and report == {pl_rpt}') 
        num_1 = num_t[num_t['ddate']==cdate]
        num_2 = num_t[num_t['ddate']==pdate]

        def create_stmt_table_pl(stmt_q, num, qtr, fp):

            stmt_table = pd.merge(stmt_q, num, how="left", left_on=['tag','version'], right_on=['tag','version'])

            if stmt_table.loc[0,'stmt'] == "IS" and "Q" in fp.upper():
                stmt_table = stmt_table[(stmt_table['qtrs'].isna()) | (stmt_table['qtrs']==1) | (stmt_table['qtrs']==0)]
            elif stmt_table.loc[0,'stmt'] == "IS" and "FY" in fp.upper():
                stmt_table = stmt_table[(stmt_table['qtrs'].isna()) | (stmt_table['qtrs']==qtr) | (stmt_table['qtrs']==0)]


            dims = {}
            for i in stmt_q[stmt_q['tag'].str.endswith("Axis")]['tag']:
                for j in stmt_q[stmt_q['tag'].str.endswith("Member")]['tag']:
                    dim_text = i.rsplit("Axis",1)[0] + "=" + j.rsplit("Member",1)[0] + ";"
                    dimh = "0x" + hashlib.md5(dim_text.encode()).hexdigest()
                    obj = {dimh : j}
                    dims.update(obj)

            for i,j in stmt_table['dimh'].items():
                if j not in [i for i in dims.keys()] + [np.nan , '0x00000000']:
                    stmt_table.drop(i,inplace=True)

            stmt_table.sort_values(['line','dimn'],ascending=[True,False], inplace=True)

            for i,j in stmt_table[['tag','version','dimh','prole','plabel']].iterrows():
                if "total" not in j['prole'].lower(): 
                    if j['dimh'] in dims and ("gaap" in j['version'].lower() or "ifrs" in j['version'].lower()):
                        new_label = j['plabel'] +" - "+ stmt_table[stmt_table['tag']=="%s"%dims[j['dimh']]]['plabel'].values[0]
                        if "[" in new_label:
                            new_label = new_label.split("[")[0].strip()
                            stmt_table.loc[i,'plabel'] = new_label
                        else:
                            stmt_table.loc[i,'plabel'] = new_label

            if True in stmt_table['tag'].duplicated().unique():
                for i, j in stmt_table[stmt_table['tag'].duplicated(False)][['prole','plabel','dimn']].iterrows():
                    if j['dimn'] == 0 and "total" not in j['plabel'].lower():
                        stmt_table.loc[i,"plabel"] = "Total " + j['plabel']

            stmt_table['value2'] = stmt_table.apply(lambda row: dcml_apply(row), axis=1)
            stmt_table['value2'] = stmt_table.apply(lambda x: x['value2'] * -1 if x['negating']==1 else x['value2'], axis=1)

            return stmt_table

        pnl_table = create_stmt_table_pl(pnl_q, num_1, qtr, fp)

        try:
            pnl_table2 = create_stmt_table(pnl_q, num_2, qtr, fp)
        except Exception as e:
            print("Previous period is having some issue:",e)
            pass

        if 'pnl_table2' in locals() and pnl_table2.empty != True:
            return [pnl_table, pnl_table2]
        else:
            return pnl_table
        
    except Exception as e:
        print("Some error occured:", e)
        pass

async def afin_stmts_2periods_bs(adsh, file_name, period, fp, qtr, cdate, pdate, pdate_bs, pdate_bs2):
    try:
        queries = [
        "SELECT * FROM pre WHERE file_name = '%s' and adsh = '%s' and stmt = 'BS' ORDER by line asc;"%(file_name, adsh),
        "SELECT tag, version, ddate, qtrs, uom, dimh, iprx, value, footnote, footlen, dimn, coreg, durp, datp, dcml FROM num where file_name = '%s' and adsh = '%s' and ddate in ('%s','%s','%s') and iprx = 0;"%(file_name, adsh, cdate, pdate, pdate_bs),
        ]

        results = await run_queries(queries)
        pre_q, num_t = results
        bs_rpt = np.min(pre_q.query('stmt=="BS"')['report'].unique())
        bs_q = pre_q.query(f'stmt=="BS" and report == {bs_rpt}') 
        
        num_1 = num_t[num_t['ddate']==cdate]
        num_bs_prev = num_t[num_t['ddate']==pdate_bs]

        def create_stmt_table_bs(stmt_q, num, qtr, fp):

            stmt_table = pd.merge(stmt_q, num, how="left", left_on=['tag','version'], right_on=['tag','version'])

            if stmt_table.loc[0,'stmt'] == "BS":
                stmt_table = stmt_table[(stmt_table['qtrs'].isna()) | (stmt_table['qtrs']==0)]

            dims = {}
            for i in stmt_q[stmt_q['tag'].str.endswith("Axis")]['tag']:
                for j in stmt_q[stmt_q['tag'].str.endswith("Member")]['tag']:
                    dim_text = i.rsplit("Axis",1)[0] + "=" + j.rsplit("Member",1)[0] + ";"
                    dimh = "0x" + hashlib.md5(dim_text.encode()).hexdigest()
                    obj = {dimh : j}
                    dims.update(obj)

            for i,j in stmt_table['dimh'].items():
                if j not in [i for i in dims.keys()] + [np.nan , '0x00000000']:
                    stmt_table.drop(i,inplace=True)

            stmt_table.sort_values(['line','dimn'],ascending=[True,False], inplace=True)

            for i,j in stmt_table[['tag','version','dimh','prole','plabel']].iterrows():
                if "total" not in j['prole'].lower(): 
                    if j['dimh'] in dims and ("gaap" in j['version'].lower() or "ifrs" in j['version'].lower()):
                        new_label = j['plabel'] +" - "+ stmt_table[stmt_table['tag']=="%s"%dims[j['dimh']]]['plabel'].values[0]
                        if "[" in new_label:
                            new_label = new_label.split("[")[0].strip()
                            stmt_table.loc[i,'plabel'] = new_label
                        else:
                            stmt_table.loc[i,'plabel'] = new_label

            if True in stmt_table['tag'].duplicated().unique():
                for i, j in stmt_table[stmt_table['tag'].duplicated(False)][['prole','plabel','dimn']].iterrows():
                    if j['dimn'] == 0 and "total" not in j['plabel'].lower():
                        stmt_table.loc[i,"plabel"] = "Total " + j['plabel']

            stmt_table['value2'] = stmt_table.apply(lambda row: dcml_apply(row), axis=1)
            stmt_table['value2'] = stmt_table.apply(lambda x: x['value2'] * -1 if x['negating']==1 else x['value2'], axis=1)

            return stmt_table

        bs_table = create_stmt_table_bs(bs_q, num_1, qtr, fp)

        try:
            bs_table2 = create_stmt_table(bs_q, num_bs_prev, qtr, fp)
        except Exception as e:
            print("Previous period is having some issue:",e)
            pass

        if 'bs_table2' in locals() and bs_table2.empty != True:
            return [bs_table,bs_table2]
        else:
            return bs_table
        
    except Exception as e:
        print("Some error occured:", e)
        pass

async def afin_stmts_2periods_cf(adsh, file_name, period, fp, qtr, cdate, pdate, pdate_bs, pdate_bs2):
    try:
        queries = [
        "SELECT * FROM pre WHERE file_name = '%s' and adsh = '%s' and stmt = 'CF' ORDER by line asc;"%(file_name, adsh),
        "SELECT tag, version, ddate, qtrs, uom, dimh, iprx, value, footnote, footlen, dimn, coreg, durp, datp, dcml FROM num where file_name = '%s' and adsh = '%s' and ddate in ('%s','%s','%s') and iprx = 0;"%(file_name, adsh, cdate, pdate, pdate_bs),
        "SELECT tag, version, ddate, qtrs, uom, dimh, iprx, value, footnote, footlen, dimn, coreg, durp, datp, dcml FROM num where file_name = '{0}' and adsh = '{1}' and ddate in ('{2}','{3}') and tag REGEXP 'Cash' and iprx = 0 and dimn = 0;".format(file_name, adsh, pdate_bs, pdate_bs2)
        ]

        results = await run_queries(queries)
        pre_q, num_t, cash_t = results
        cf_rpt = np.min(pre_q.query('stmt=="CF"')['report'].unique())
        cf_q = pre_q.query(f'stmt=="CF" and report == {cf_rpt}') 
        num_1 = num_t[num_t['ddate']==cdate]
        num_2 = num_t[num_t['ddate']==pdate]
        cash = cash_t[cash_t['ddate']==pdate_bs]
        cash2 = cash_t[cash_t['ddate']==pdate_bs2]


        def create_stmt_table_cf(stmt_q, num, qtr, fp):

            stmt_table = pd.merge(stmt_q, num, how="left", left_on=['tag','version'], right_on=['tag','version'])
            
            if stmt_table.loc[0,'stmt'] == "CF":
                stmt_table = stmt_table[(stmt_table['qtrs'].isna()) | (stmt_table['qtrs']==qtr) | (stmt_table['qtrs']==0)]

            dims = {}
            for i in stmt_q[stmt_q['tag'].str.endswith("Axis")]['tag']:
                for j in stmt_q[stmt_q['tag'].str.endswith("Member")]['tag']:
                    dim_text = i.rsplit("Axis",1)[0] + "=" + j.rsplit("Member",1)[0] + ";"
                    dimh = "0x" + hashlib.md5(dim_text.encode()).hexdigest()
                    obj = {dimh : j}
                    dims.update(obj)

            for i,j in stmt_table['dimh'].items():
                if j not in [i for i in dims.keys()] + [np.nan , '0x00000000']:
                    stmt_table.drop(i,inplace=True)

            stmt_table.sort_values(['line','dimn'],ascending=[True,False], inplace=True)

            for i,j in stmt_table[['tag','version','dimh','prole','plabel']].iterrows():
                if "total" not in j['prole'].lower(): 
                    if j['dimh'] in dims and ("gaap" in j['version'].lower() or "ifrs" in j['version'].lower()):
                        new_label = j['plabel'] +" - "+ stmt_table[stmt_table['tag']=="%s"%dims[j['dimh']]]['plabel'].values[0]
                        if "[" in new_label:
                            new_label = new_label.split("[")[0].strip()
                            stmt_table.loc[i,'plabel'] = new_label
                        else:
                            stmt_table.loc[i,'plabel'] = new_label

            if True in stmt_table['tag'].duplicated().unique():
                for i, j in stmt_table[stmt_table['tag'].duplicated(False)][['prole','plabel','dimn']].iterrows():
                    if j['dimn'] == 0 and "total" not in j['plabel'].lower():
                        stmt_table.loc[i,"plabel"] = "Total " + j['plabel']

            stmt_table['value2'] = stmt_table.apply(lambda row: dcml_apply(row), axis=1)
            stmt_table['value2'] = stmt_table.apply(lambda x: x['value2'] * -1 if x['negating']==1 else x['value2'], axis=1)

            return stmt_table

        cf_table = create_stmt_table_cf(cf_q, num_1, qtr, fp)

        if True in cf_table['prole'].str.contains("Start").unique():
            cash_value = cash[(cash['tag'] == cf_table.loc[cf_table['prole'].str.contains("Start"),'tag'].values[0]) & (cash['version'] == cf_table.loc[cf_table['prole'].str.contains("Start"),'version'].values[0])]
            cf_table.loc[cf_table['prole'].str.contains("Start"),['ddate','qtrs','uom','dimh','iprx','value','footnote','footlen','dimn','coreg','durp','datp','dcml']] = cash_value[['ddate','qtrs','uom','dimh','iprx','value','footnote','footlen','dimn','coreg','durp','datp','dcml']].values
            cf_table['value2'] = cf_table.apply(lambda row: dcml_apply(row), axis=1)
            cf_table['value2'] = cf_table.apply(lambda x: x['value2'] * -1 if x['negating']==1 else x['value2'], axis=1)

        try:
            cf_table2 = create_stmt_table(cf_q, num_2, qtr, fp)

            if True in cf_table2['prole'].str.contains("Start").unique():
                cash_value2 = cash2[(cash2['tag'] == cf_table2.loc[cf_table2['prole'].str.contains("Start"),'tag'].values[0]) & (cash2['version'] == cf_table2.loc[cf_table2['prole'].str.contains("Start"),'version'].values[0])]
                cf_table2.loc[cf_table2['prole'].str.contains("Start"),['ddate','qtrs','uom','dimh','iprx','value','footnote','footlen','dimn','coreg','durp','datp','dcml']] = cash_value2[['ddate','qtrs','uom','dimh','iprx','value','footnote','footlen','dimn','coreg','durp','datp','dcml']].values
                cf_table2['value2'] = cf_table2.apply(lambda row: dcml_apply(row), axis=1)
                cf_table2['value2'] = cf_table2.apply(lambda x: x['value2'] * -1 if x['negating']==1 else x['value2'], axis=1)

        except Exception as e:
            print("Previous period is having some issue:",e)
            pass

        if 'cf_table2' in locals() and cf_table2.empty != True:
            return [cf_table,cf_table2]
        else:
            return cf_table
        
    except Exception as e:
        print("Some error occured:", e)
        pass

# adsh, file_name, period, fp, qtr, cdate, pdate_bs = "0000950170-23-003234", "2023_02_notes", "2022-12-31", "FY", 4, "2022-12-31", "2021-12-31" 
# all_stmts = asyncio.run(afin_stmts(adsh, file_name, period, fp, qtr, cdate, pdate_bs))
# print(all_stmts)

def unique_pl_items(df_pl):
    uq = {}
    df_pl['tag_small'] = df_pl['tag'].str.lower()

    for i,tag_small_case in df_pl['tag_small'].items():
        if "statementlineitem" in tag_small_case:
            uq['start'] = i
        elif "cost" in tag_small_case or "expense" in tag_small_case:
            uq['cost'] = i
            break     

    for i,tag_small_case in df_pl['tag_small'].items():
        if "incometaxexpensebenefit" == tag_small_case:
            uq['tax_start'] = i
            break
        elif "netincomeloss" == tag_small_case:
            uq['tax_start'] = i
            break

    for i,tag_small_case in df_pl['tag_small'].items():
        if "netincomeloss" == tag_small_case:
            uq['net_il'] = i
            break

    for i,tag_small_case in df_pl['tag_small'].items():
        if "per" in tag_small_case and "share" in tag_small_case:
            uq['pershare'] = i
            break

    return uq

def missing_labels(df):
    for i,(j,k) in df[['line_x','line_y']].iterrows():
        if np.isnan(j):
            df.loc[i,'line_x'] = k

    for i,(j,k) in df[['plabel_x','plabel_y']].iterrows():
        if isinstance(j,float):
            df.loc[i,'plabel_x'] = k

    df.sort_values('line_x',inplace=True)
    return df

def merge_pl(df_pl1,sub_pl):
    
    df_pl2 = sub_pl.query('stmt=="IS"')
    dl2 = unique_pl_items(df_pl2)

    rev1 = df_pl1.query('type=="rev"')
    rev2 = df_pl2.loc[dl2.get('start'):dl2.get('cost')-1]

    rev = pd.merge(rev1,rev2[['tag','line','dimh','plabel'] + rev2.filter(like='dat').columns.to_list()],
            on=['tag','dimh'],how='outer')
    
    rev = missing_labels(rev)
    rev['type'] = 'rev'

    exp1 = df_pl1.query('type=="exp"')
    exp2 = df_pl2.loc[dl2.get('cost'):dl2.get('tax_start')-1]

    exp = pd.merge(exp1,exp2[['tag','line','dimh','plabel'] + exp2.filter(like='dat').columns.to_list()],
            on=['tag','dimh'],how='outer')
    
    exp = missing_labels(exp)
    exp['type'] = 'exp'

    taxil1 = df_pl1.query('type=="taxni"')
    taxil2 = df_pl2.loc[dl2.get('tax_start'):dl2.get('net_il')]

    taxni = pd.merge(taxil1,taxil2[['tag','line','dimh','plabel'] + taxil2.filter(like='dat').columns.to_list()],
            on=['tag','dimh'],how='outer')
    
    taxni = missing_labels(taxni)
    taxni['type'] = 'taxni'

    posttax1 = df_pl1.query('type=="posttax"')
    posttax2 = df_pl2.loc[dl2.get('net_il')+1:dl2.get('pershare')-1]

    posttax = pd.merge(posttax1,posttax2[['tag','line','dimh','plabel'] + posttax2.filter(like='dat').columns.to_list()],
            on=['tag','dimh'],how='outer')
    
    posttax = missing_labels(posttax)
    posttax['type'] = 'posttax'

    supp1 = df_pl1.query('type=="supp"')
    supp2 = df_pl2.loc[dl2.get('pershare'):]

    supp = pd.merge(supp1,supp2[['tag','line','dimh','plabel'] + supp2.filter(like='dat').columns.to_list()],
            on=['tag','dimh'],how='outer')
    
    supp = missing_labels(supp)
    supp['type'] = 'supp'

    com_df = pd.concat([rev,exp,taxni,posttax,supp])
    com_df.drop(['line_y','plabel_y'],axis=1,inplace=True)
    com_df.rename(columns={'line_x':'line','plabel_x':'plabel'},inplace=True)   
    
    return com_df

# df_all = [Dataframes of all one year financials which will be merged]
def arrange_stmt(df_all):    
    merged_df = pd.DataFrame()
    try:    
        for i in df_all:
            i.drop(['coreg','footlen','footnote','datp'],axis=1,inplace=True)
            i.dropna(subset='value',inplace=True)
            date = pd.to_datetime(i.iloc[0]['ddate'],unit='ms').date()
            i.rename(columns={'ddate':'date_%s'%date,'value':'data_%s'%date,'value2':'data2_%s'%date},inplace=True)
            
        df_final = pd.merge(df_all[0], df_all[1], on=['adsh', 'file_name', 'inpth', 'line', 'negating', 'plabel', 'prole','report', 'stmt',
                                                    'tag', 'version','dcml','dimh', 'dimn', 'durp', 'iprx', 'qtrs','uom'], how='left')
        
        df_pl1 = df_final.query('stmt=="IS"')
        
        df_pl2 = df_all[2].query('stmt=="IS"')


        dl1 = unique_pl_items(df_pl1)
        dl2 = unique_pl_items(df_pl2)

        rev1 = df_pl1.loc[dl1.get('start'):dl1.get('cost')-1]
        rev2 = df_pl2.loc[dl2.get('start'):dl2.get('cost')-1]

        rev = pd.merge(rev1,rev2[['tag','line','dimh','plabel'] + rev2.filter(like='dat').columns.to_list()],
                on=['tag','dimh'],how='outer')
        
        rev = missing_labels(rev)
        rev['type'] = 'rev'
        

        exp1 = df_pl1.loc[dl1.get('cost'):dl1.get('tax_start')-1]
        exp2 = df_pl2.loc[dl2.get('cost'):dl2.get('tax_start')-1]

        exp = pd.merge(exp1,exp2[['tag','line','dimh','plabel'] + exp2.filter(like='dat').columns.to_list()],
                on=['tag','dimh'],how='outer')
        
        exp = missing_labels(exp)
        exp['type'] = 'exp'

        taxil1 = df_pl1.loc[dl1.get('tax_start'):dl1.get('net_il')]
        taxil2 = df_pl2.loc[dl2.get('tax_start'):dl2.get('net_il')]

        taxni = pd.merge(taxil1,taxil2[['tag','line','dimh','plabel'] + taxil2.filter(like='dat').columns.to_list()],
                on=['tag','dimh'],how='outer')
        
        taxni = missing_labels(taxni)
        taxni['type'] = 'taxni'

        posttax1 = df_pl1.loc[dl1.get('net_il')+1:dl1.get('pershare')-1]
        posttax2 = df_pl2.loc[dl2.get('net_il')+1:dl2.get('pershare')-1]

        posttax = pd.merge(posttax1,posttax2[['tag','line','dimh','plabel'] + posttax2.filter(like='dat').columns.to_list()],
                on=['tag','dimh'],how='outer')
        
        posttax = missing_labels(posttax)
        posttax['type'] = 'posttax'

        supp1 = df_pl1.loc[dl1.get('pershare'):]
        supp2 = df_pl2.loc[dl2.get('pershare'):]

        supp = pd.merge(supp1,supp2[['tag','line','dimh','plabel'] + supp2.filter(like='dat').columns.to_list()],
                on=['tag','dimh'],how='outer')
        
        supp = missing_labels(supp)
        supp['type'] = 'supp'

        combined_df = pd.concat([rev,exp,taxni,posttax,supp])
        combined_df.drop(['line_y','plabel_y'],axis=1,inplace=True)
        combined_df.rename(columns={'line_x':'line','plabel_x':'plabel'},inplace=True)   
        
        merged_df = pd.concat([combined_df,merged_df],axis=0,ignore_index=True)
        
        try:
            if len(df_all) >= 4 :
                for df in df_all[3:]:
                    merged_df = merge_pl(merged_df,df)
        except Exception as e:
            print(f"Some error occured in loop: {e}")
            pass

        return merged_df
    except Exception as e:
        print(f"Some error occured: {e}")
        pass



