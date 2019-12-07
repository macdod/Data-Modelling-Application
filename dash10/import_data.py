import mysql.connector
import numpy as np
import pandas as pd
from datetime import date, datetime
from . import oracledb
from . import mysqldb
 


def execute_tb(host,port,user,passwd,dbname, x):
    mydb = mysql.connector.connect(
        host=host,
        user=user,
        passwd=passwd,
        database=dbname
    )

    mycursor = mydb.cursor()
    mycursor.execute(x)


def jointable(host, port, user, passwd, dbname, subject_area, tables, att, joinTable, joinColumn):
    print(joinTable)
    print(joinColumn)
    
    x = len(joinTable)
    join = '';
    join+= joinTable[0]+'1.'+joinColumn[0]+'='+joinTable[1]+'1.'+joinColumn[1]
    for i in range(2, x-1, 2):
        join+= ' AND ' + joinTable[i]+'1.'+joinColumn[i]+'='+joinTable[i+1]+'1.'+joinColumn[i+1]

    print(join)
    join+=';'
    
    tmp = 'CREATE OR REPLACE VIEW '+subject_area+' as SELECT'
    for i in range(len(tables)):
        tb = tables[i]+'1'
        s=' '
        cols = att[i]
        for j in cols:
            s+=tb+'.'+j
            s+=' AS '
            s+=tb+'_'+j+','
            s.rstrip(',')
        tmp+=s
    tmp=tmp.rstrip(',')
    tmp+=' FROM '
    for i in tables:
        tmp+=i+'1,'
    tmp=tmp.rstrip(',')
    tmp+=' WHERE '
    
    tmp+=join
    print(tmp)
    execute_tb('localhost',3306,'root','','dashdesk',tmp)
    
#jointable()    
  


def importFromOracle(host, port, user, passwd, dbname, tables, att, dtypes, joinTable, joinColumn, subject_area):
    print("insert ------- >")
    for i in range(0, len(tables)):
        data = oracledb.getdata(host,port,user,passwd,dbname,tables[i], att[i])
        data = np.transpose(data)
        print(data)

        for k in range(0, len(data)):
            s = 'INSERT INTO ' + tables[i] + '1 VALUES ('
            x = str(data[k][0])

            if dtypes[i][0] == 'VARCHAR(255)' or dtypes[i][0] == 'DATETIME':
                x = '\'' + x + '\''
            print("DATA YE HAI----->>>")
            print(type(x))
            s = s + x

            for j in range(1, len(data[k])):
                y = str(data[k][j])

                if dtypes[i][j] == 'VARCHAR(255)' or dtypes[i][j] == 'DATETIME':
                    y = '\'' + y + '\''

                s += ',' + y
            s += ');'
            print(s)
            execute_tb('localhost', 3306, 'root', '', 'dashdesk', s)
    jointable('localhost', 3306, 'root', '', 'dashdesk', subject_area, tables, att, joinTable, joinColumn)



def importFromMysql(host, port, user, passwd, dbname, tables, att, dtypes, joinTable, joinColumn, subject_area):
    print("insert ------- >")
    for i in range(0, len(tables)):
        data = mysqldb.getdata(host,port,user,passwd,dbname,tables[i], att[i])
        data = np.transpose(data)
        print(data)

        for k in range(0, len(data)):
            s = 'INSERT INTO '+tables[i]+'1 VALUES ('
            x=str(data[k][0])
            
            if dtypes[i][0]=='VARCHAR(255)' or dtypes[i][0]=='DATETIME':
                x='\''+x+'\''
            print("DATA YE HAI----->>>")
            print(type(x))   
            s=s+x    
                
            for j in range(1, len(data[k])):
                y=str(data[k][j])
                
                if dtypes[i][j]=='VARCHAR(255)' or dtypes[i][j]=='DATETIME':
                    y= '\''+y+'\''
                
                s+=',' +y
            s+=');'
            print(s)
            execute_tb('localhost', 3306, 'root', '','dashdesk', s)
    jointable('localhost', 3306, 'root', '', 'dashdesk', subject_area, tables, att, joinTable, joinColumn)



def Import_data_model(path, path1,host, port, user, passwd, dbtype, dbname):
    print("abcd")
    print(path)
    print(path1)
    print(host)
    print(port)
    print(user)
    print(passwd)
    print(dbtype)
    print(dbname)
    df=pd.read_csv(path)
    
    n,m=df.shape
    
    for i in range(n):
        for j in range(m-2):
            df.iloc[i,j]=df.iloc[i,j].replace(' ','_')

    att = df.groupby(['Subject_Area', 'Entity_Logical_Name'])['Attribute_Logical_Name'].apply(list)
    dtypes = df.groupby(['Subject_Area', 'Entity_Logical_Name'])['Data_Type'].apply(list)
    key = df.groupby(['Subject_Area', 'Entity_Logical_Name'])['KEY_IND'].apply(list)

    result = pd.concat([att, dtypes, key], axis = 1)
    
    result_temp = result.reset_index()
    
    tables=list(result_temp['Entity_Logical_Name'])

    
    tb_queries=[]

    dtypes = list(result_temp['Data_Type'])
    for i in range(0, len(dtypes)):
        for j in range(0, len(dtypes[i])):
            if dtypes[i][j].upper().find('NUMBER')!=-1:
                dtypes[i][j]='FLOAT'
            if dtypes[i][j].upper().find('VARCHAR')!=-1:
                dtypes[i][j]='VARCHAR(255)'
            if dtypes[i][j].upper().find('DATETIME')!=-1:
                dtypes[i][j]='DATETIME'

    j=0
    att = list(result_temp['Attribute_Logical_Name'])
    print(att)
    print(dtypes)

    print("create ----  > ")
    for i,tb in enumerate(tables):
            s='create table '+tb+'1'+'('
            for attribute,data_type,key_check in zip(att[i],dtypes[i],key[i]):
                    if j==0:
                        s+=attribute+' '+data_type
                    else:
                        s+=','+attribute+' '+data_type
                    if key_check=='KEY' or key_check=='PRI':
                        s+=' Primary Key'
                    j+=1
            s+=');'
            tb_queries.append(s)
            print(s)
            x = 'drop table if exists ' + tb +'1;'
            execute_tb('localhost', 3306, 'root', '','dashdesk', x)
            execute_tb('localhost', 3306, 'root', '','dashdesk', s)
            j=0
            
    subject_area, b, c, d, e, joinTable, joinColumn = Import_data_model_Hybrid(path, path1)
    
    
    #for importing from CSV   
    print("joinTable")
    print(joinTable)
    print(joinColumn)
    if dbtype=='oracle':   
        importFromOracle(host, port, user, passwd, dbname, tables, att,dtypes, joinTable, joinColumn, subject_area)
    elif dbtype == 'mysql':
        importFromMysql(host, port, user, passwd, dbname, tables, att,dtypes, joinTable, joinColumn, subject_area)

    return subject_area
        
    """    
    elif dbtype=='sql':
        importFromSQL(tables, cols, att)
    """
    
    """
    for i in tb_queries:
        i=i.replace('datetime(6)', 'Varchar(255)')
        print(i)
        execute_tb('localhost',3306,'root','','da',i)
    """
    """
    data=''
    print("data");
    data+='LOAD DATA INFILE '+ '\'C:/wamp64/tmp/iris.csv\'' +' INTO TABLE iris '+'FIELDS TERMINATED BY ' + '\',\'' + ' ENCLOSED BY '+ '\'\"\''+' LINES TERMINATED BY '+'\'\\n\''+' IGNORE 1 ROWS;'
    print("data1")
    print(data)
    execute_tb('localhost',3306,'root','','da',data)
    data='LOAD DATA INFILE '+'\'C:/wamp64/tmp/data.csv\''+ ' INTO TABLE dashboard_sales '+'FIELDS TERMINATED BY ' + '\',\'' + ' ENCLOSED BY '+ '\'\"\''+' LINES TERMINATED BY '+'\'\\n\''+' IGNORE 1 ROWS;'
    print("data2")
    print(data)
    execute_tb('localhost',3306,'root','','da',data)
    """
    



"""
def Export_Data_Model(var, tables, columns, dtypes, keys, joinTable, joinColumn):
    sa=[]
    x = len(tables)
    for i in range(x):
        sa.append(var)
    df = {'Subject Area':[i for i in sa], 'Entity Logical Model':[i for i in tables], 'Attribute Logical Name':[i for i in columns], 'Data Type':[i for i in dtypes], 'KEY_IND':[i for i in keys]}
    df = pd.DataFrame(df)
    df.to_csv('C:/Users/adityshe/Desktop/check.csv', index=False)
    
    x = len(joinTable)
    join_list=[]
    for i in range(0, x-1, 2):
        join_list.append(joinTable[i]+'.'+joinColumn[i]+'='+joinTable[i+1]+'.'+joinColumn[i+1])
        
    df_join = {'Subject Area':['Join Condition Type'+str(i) for i in range(int(x/2))], 'Joins':join_list}
    df_join = pd.DataFrame(df_join)
    df_join.to_csv('C:/Users/adityshe/Desktop/checkJoin.csv', index=False)
    
Export_Data_Model('cust', ['A', 'B'], ['c1', 'c2'], ['d1', 'd2'], [' ', 'KEY'], ['t1', 't2', 't2', 't3'], ['c1', 'c2', 'c3', 'c4'])
"""

def Import_data_model_Hybrid(path,path1):
    df=pd.read_csv(path)
    #df_join=pd.read_csv(path1)
    
    n,m=df.shape
    for i in range(n):
        for j in range(m-2):
            df.iloc[i,j]=df.iloc[i,j].replace(' ','_')
    df.rename(columns={'Subject Area':'Subject_Area', 'Entity Logical Name':'Entity_Logical_Name', 'Attribute Logical Name':'Attribute_Logical_Name',
       'Data Type':'Data_Type'}, inplace=True)
    print(df.columns)
    att = df.groupby(['Subject_Area', 'Entity_Logical_Name'])['Attribute_Logical_Name'].apply(list)
    dtypes = df.groupby(['Subject_Area', 'Entity_Logical_Name'])['Data_Type'].apply(list)
    key = df.groupby(['Subject_Area', 'Entity_Logical_Name'])['KEY_IND'].apply(list)
    
    result = pd.concat([att, dtypes, key], axis = 1)
    result_temp = result.reset_index()
    
    x = [str(i) for i in df['Subject_Area'].unique()]
    subject_area = x[0]
    tables=list(result_temp['Entity_Logical_Name'])
    cols=[]
    dt=[]
    keys=[]
    for i,tb in enumerate(tables):
        s='create table '+tb+'('
        cols.append(att[i])
        dt.append(dtypes[i])
        keys.append(key[i])
    df1=pd.read_csv(path1)
    n,m=df1.shape
    for i in range(n):
        for j in range(m):
            df1.iloc[i,j]=df1.iloc[i,j].replace(' ','_')
    df1.rename(columns={'Subject Area':'Subject_Area'},inplace=True)
    j_cond = df1.groupby(['Subject_Area'])['Joins'].apply(list)
    j_tabs = j_cond.index
    j_tab=[]
    j_cols=[]
    for i,tab in enumerate(j_tabs):
        for cond in j_cond[i]:
            c1=cond.split('=')
            c11=c1[0].split('.')
            c12=c1[1].split('.')
            j_tab.append(c11[0])
            j_tab.append(c12[0])
            j_cols.append(c11[1])
            j_cols.append(c12[1])
    return subject_area,tables,cols,dt,keys ,j_tab,j_cols          
            
        
#Import_data_model_Hybrid()


        