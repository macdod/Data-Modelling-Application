import numpy as np
import pandas as pd
'''
def Import_data_model(path='C:/Users/adityshe/Desktop/data_model.csv',path1='C:/Users/adityshe/Desktop/joins.csv'):
    df=pd.read_csv(path)
    n,m=df.shape
    for i in range(n):
        for j in range(m-2):
            df.iloc[i,j]=df.iloc[i,j].replace(' ','_')
    att = df.groupby(['Subject_Area', 'Entity_Logical_Name'])['Attribute Logical Name'].apply(list)
    dtypes = df.groupby(['Subject_Area', 'Entity_Logical_Name'])['Data Type'].apply(list)
    key = df.groupby(['Subject_Area', 'Entity_Logical_Name'])['KEY_IND'].apply(list)

    result = pd.concat([att, dtypes, key], axis = 1)
    result_temp = result.reset_index()

    tables=list(result_temp['Entity_Logical_Name'])  
    queries=[]
    a , b = result.shape
    tb_queries=[]
    j=0
    for i,tb in enumerate(tables):
        s='create table '+tb+'('
        for attribute,data_type,key_check in zip(att[i],dtypes[i],key[i]):
            if j==0:
                s+=attribute+' '+data_type
            else:
                s+=','+attribute+' '+data_type
            if key_check=='KEY':
                s+=' Primary Key'
            j+=1
        s+=')'
        tb_queries.append(s)
        j=0
    df1=pd.read_csv(path1)
    n,m=df1.shape
    for i in range(n):
        for j in range(m):
            df1.iloc[i,j]=df1.iloc[i,j].replace(' ','_')
    j_cond = df1.groupby(['Subject_Area'])['Joins'].apply(list)
    j_tabs = j_cond.index
'''
'''for i,tab in enumerate(j_tabs):
    s='create view '+tab+' as '
    s+='select *'
    for 
    for cond in j_cond[i]:
'''
            
def Export_Data_Model(var, tables, columns, dtypes, keys, joinTable, joinColumn, path, filename1, filename2):
    sa=[]
    x = len(tables)
    for i in range(x):
        sa.append(var)
    df = {'Subject_Area':[i for i in sa], 'Entity_Logical_Name':[i for i in tables], 'Attribute_Logical_Name':[i for i in columns], 'Data_Type':[i for i in dtypes], 'KEY_IND':[i for i in keys]}
    df = pd.DataFrame(df)
    df.to_csv(path + '/' + filename1 + '.csv', index=False)

    x = len(joinTable)
    join_list=[]
    for i in range(0, x-1, 2):
        join_list.append(joinTable[i]+'.'+joinColumn[i]+'='+joinTable[i+1]+'.'+joinColumn[i+1])
        
    df_join = {'Subject_Area':[var for i in range(int(x/2))], 'Joins':join_list}
    df_join = pd.DataFrame(df_join)
    df_join.to_csv(path + '/' + filename2 + '.csv', index=False)
    



def Import_data_model_Hybrid(path="C:/Users/rishiksa/Desktop/projects/dashdesk/dash10/static/uploads/rishikesh1410/datamodel/datamodel.csv",path1="C:/Users/rishiksa/Desktop/projects/dashdesk/dash10/static/uploads/rishikesh1410/datamodel/datamodeljoin.csv"):
    print("pathhhhh")
    print(path)
    print(path1)
    df=pd.read_csv(path)
    #df_join=pd.read_csv(path1)
    
    n,m=df.shape
    for i in range(n):
        for j in range(m-2):
            df.iloc[i,j]=df.iloc[i,j].replace(' ','_')
    df.rename(columns={'Subject Area':'Subject_Area','Entity Logical Name':'Entity_Logical_Name','Attribute Logical Name':'Attribute_Logical_Name','Data Type':'Data_Type'},inplace=True)
    print (df.columns)
    att = df.groupby(['Subject_Area', 'Entity_Logical_Name'])['Attribute_Logical_Name'].apply(list)
    dtypes = df.groupby(['Subject_Area', 'Entity_Logical_Name'])['Data_Type'].apply(list)
    key = df.groupby(['Subject_Area', 'Entity_Logical_Name'])['KEY_IND'].apply(list)
    
    result = pd.concat([att, dtypes, key], axis = 1)
    result_temp = result.reset_index()
    
    x = [str(i) for i in df['Subject_Area'].unique()]
    subject_area = x[0]
    tables=list(result_temp['Entity_Logical_Name'])
    cols=[]
    keys=[]
    dt=[]
    tables1=list(df['Entity_Logical_Name'])
    cols1=list(df['Attribute_Logical_Name'])
    dt1=list(df['Data_Type'])
    keys1=list(df['KEY_IND'])
    for i,tb in enumerate(tables):
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
    j_tabs = list(j_cond.index)
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
    return subject_area,tables1,cols1,dt1,keys1,tables,j_tab,j_cols
            
            
            





        