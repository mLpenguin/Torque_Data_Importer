#!/usr/bin/env python
# coding: utf-8

# In[1]:


DATA_PATH = './Data'


# In[2]:


import sqlite3 as sq


# In[3]:


databaseName = 'torque.db'
importedTableName = 'importedFolderName'


# # Check If New Files To Import

# In[4]:


import os
from glob import glob


# In[5]:


def isDataInLog(dataEntry):
    
    
    con = sq.connect(databaseName)
    cur = con.cursor()
    try:
        cur.execute(f"""SELECT Folder
                    FROM {importedTableName}
                    WHERE folder={dataEntry}""")
    except:
        result=False

    # Fetch one result from the query because it
    # doesn't matter how many records are returned.
    # If it returns just one result, then you know
    # that a record already exists in the table.
    # If no results are pulled from the query, then
    # fetchone will return None.
    result = cur.fetchone()
    
    con.close()

    if result:
        return True
    else:
        return False


# In[6]:


Filter = "trackLog.csv"
all_csv_files = [file
                 for path, subdir, files in os.walk(DATA_PATH)
                 for file in glob(os.path.join(path, Filter))]


# In[7]:


print(f"{len(all_csv_files)} files in data folder")


# In[8]:


dataToImport=[]

for x in range(len(all_csv_files)):
    folderName = all_csv_files[x].split('\\')[-2]
    
    if not isDataInLog(folderName):
        dataToImport.append(all_csv_files[x])
        


# In[9]:


if len(dataToImport)==0:
    print("No files left to import. Quitting")
    exit()
else:
    print(f"Importing {len(dataToImport)} files")


# # Convert CSV to Dataframe

# In[10]:


import pandas as pd
import numpy as np


# In[11]:


import csv
def readCSV(fileName):
    outputArray = []
    with open(fileName, 'r') as file:
        reader = csv.reader(file, quoting=csv.QUOTE_ALL, skipinitialspace=True)
        header = next(reader)
        #next(reader, None)  # skip the headers
        
        for row in reader:
            temp = []
            for i in range(0,len(row)):
                if row[i] != '':
                    temp.append(row[i])
            #print (row[0])
            outputArray.append(temp)
    return outputArray, header


# In[12]:


def cleanUpDf(dataframe):

    df = pd.concat(dataframe, axis=0, ignore_index=True)
    df = df.drop('GPS Time', axis=1)
    df['Device Time'] = pd.to_datetime(df['Device Time'])
    df = df.set_index('Device Time')
    df = df.replace(' ', "_")

    #replace null values with nan
    df = df.replace('-', np.NaN)

    #clean up column names
    try:
        df['Speed (OBD)(km/h)'] = df['Speed (OBD)(km/h)'].str.replace(",", "").astype(float)
    except:
        pass
    try:
        df.columns = df.columns.str.replace('Â', '') #fix temp column names
    except:
        pass
    try:
        df.columns = df.columns.str.replace('â‚‚', '2') #fix co2 column names
    except:
        pass
    
    df = df.apply(pd.to_numeric)

    return df


# In[13]:


masterData = []


for x in range(len(dataToImport)):
    data, header = readCSV(dataToImport[x])
    if len(data) != 0:
        df = pd.DataFrame(data = data, 
                    #index = ["Row_1", "Row_2"], 
                    columns = header)
        df['Start Drive Time'] = dataToImport[x].split('\\')[-2]
        masterData.append(df)
    
df = cleanUpDf(masterData)


# In[14]:


df.head(2)


# In[15]:


for col in df.columns:
    print(col)


# In[16]:


df.dtypes


# # Move Read Data to Compleated Folder
import shutilfor x in range(len(all_csv_files)):
    folderName = all_csv_files[x].split('\\')
    folderName='\\'.join(folderName[len(folderName)-2:-1])
    target = COMPLEATED_PATH+'\\'+folderName
    original = IMPORT_PATH+'\\'+folderName
    shutil.move(original, target)
# # Convert Dataframe to Database

# In[17]:


tableName = "logData"
tempTableName = "importData"


# In[18]:


def sqlColumnFormat(l):
    
    temp = []
    
    for x in range(len(l)):
        temp.append(f"\"{l[x]}\"")
    
    output = ','.join(temp)
    return output


# In[19]:


con = sq.connect(databaseName)
cur = con.cursor()
cur.execute(f'''SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{tableName}' ''')

#if the count is 1, then table exists
if cur.fetchone()[0]==0:

    print('Table doesn\'t exist')
    #Create new table
    df.to_sql(tableName, con, if_exists='append', index=True) # writes to database
    
    with con: #Make unique
        con.execute("DROP INDEX \"ix_logData_Device Time\"")
        con.execute("CREATE UNIQUE INDEX \"ix_logData_Device Time\" ON \"logData\" (\"Device Time\")")

else:
    print('Table exists. Appending')
    df.to_sql(tempTableName, con, if_exists='replace', index=True) # writes to database
    
    #get column names
    cur.execute("select * from %s where 1=0;" % "logData")
    logDataColumns = [d[0] for d in cur.description]
    cur.execute("select * from %s where 1=0;" % "importData")
    importDataColumns = [d[0] for d in cur.description]
    
    newColumns = list(set(importDataColumns) - set(logDataColumns))
    
    if len(newColumns) >= 1:
    
        print(f"New Columns: {sqlColumnFormat(newColumns)}")

        for x in range(len(newColumns)):
            columnName = newColumns[x]
        
            with con:
                con.execute(f"ALTER TABLE {tableName} ADD COLUMN \"{columnName}\" TEXT")
            print(f"Adding column {columnName} to {tableName}")
    
    
    mergeData = f'''INSERT into {tableName} ({sqlColumnFormat(importDataColumns)})
    SELECT {sqlColumnFormat(importDataColumns)}
    FROM {tempTableName} 
    WHERE "Device Time" NOT IN (SELECT {tableName}."Device Time" FROM {tableName});'''

    #print (mergeData)
    with con:
        con.execute(mergeData)
        con.execute(f"DROP TABLE IF EXISTS {tempTableName};")

print("Done")
con.close() # good practice: close connection


# In[ ]:





# # Add Processed Data To List

# In[20]:


tmp = []
for x in range(len(dataToImport)):
    tmp.append(dataToImport[x].split('\\')[-2])

importedFolders = pd.DataFrame(tmp, columns =['Folder']).apply(pd.to_numeric)
importedFolders = importedFolders.set_index('Folder')


# In[21]:


con = sq.connect(databaseName)
cur = con.cursor()
cur.execute(f'''SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{importedTableName}' ''')

#if the count is 1, then table exists
if cur.fetchone()[0]==0:

    print('Table doesn\'t exist')
    #Create new table
    importedFolders.to_sql(importedTableName, con, if_exists='append', index=True) # writes to database
    
    with con: #Make unique
        con.execute("DROP INDEX \"ix_importedFolderName_Folder\"")
        con.execute("CREATE UNIQUE INDEX \"ix_importedFolderName_Folder\" ON \"importedFolderName\" (\"Folder\")")

else:
    print('Table exists. Appending')
    importedFolders.to_sql(tempTableName, con, if_exists='replace', index=True) # writes to database
    
    
    mergeData = f'''INSERT into {importedTableName} (Folder)
    SELECT Folder
    FROM {tempTableName} 
    WHERE "Folder" NOT IN (SELECT {importedTableName}."Folder" FROM {importedTableName});'''

    #print (mergeData)
    with con:
        con.execute(mergeData)
        con.execute(f"DROP TABLE IF EXISTS {tempTableName};")
    
con.close()


# In[ ]:




