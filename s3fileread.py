###
#
# Compare last 2 days of inventory/product records (AWS S3 Storage, Glue Job)
# MODULAR FILE LOCATE
#
###

import boto3
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
import pytz

load_dotenv()

BUCKET_NAME = os.environ.get('BUCKET_NAME')

#today = datetime.datetime.strptime('07/12/2023', '%m/%d/%Y')
today = datetime.now(tz= timezone.utc)
ysday = (today + timedelta(days=-1))
# todayDay = today.strftime('%Y%m%d')
# print(today)
# print(type(ysday))

#today = (today).strftime("%Y%m%d")

srcFolder = 's3://' + BUCKET_NAME + '/BIDW/'
#newFile = 's3://' + BUCKET_NAME + '/BIDW/new.csv'
print(srcFolder)

def filterPaths(objects, path):
    returnList = []

    for objPath in objects:

        baseStr = 'RBH_ProductMaster_'
        objStr = objPath[len(path):]
        objStr = objStr[:18]

        if baseStr == objStr:
            returnList.append(objPath)

    return returnList

def findCompareFiles(objects, today, folderPath):
    retListTemp = []
    returnList = []

    # Extract update files
    for objPath in objects:
        if str(objPath).startswith(folderPath + 'RBH_ProductMaster_'):
            retListTemp.append(objPath)

    # Sort files
    retListTemp.sort(reverse= True)

    # Find newest file for today
    for objPath in retListTemp:
        if str(objPath).startswith(folderPath + 'RBH_ProductMaster_' + today.strftime('%Y%m%d')):
            returnList.append(str(objPath))
            break
    
    # Find newest file for yesterday
    for objPath in retListTemp:
        if str(objPath).startswith(folderPath + 'RBH_ProductMaster_' + (today + timedelta(days=-1)).strftime('%Y%m%d')):
            returnList.append(str(objPath))
            break

    return returnList

suff = 'RBH_ProductMaster_'+'*'
print(suff)


import awswrangler as wr
objects = wr.s3.list_objects(srcFolder, last_modified_begin= ysday)
print(objects)
print()

objectsT2 = findCompareFiles(objects= objects, today= today, folderPath= srcFolder)
print(objectsT2, '\n')

newFile = objectsT2[0]
srcFile = objectsT2[1]
print(newFile, '\n', srcFile, '\n')

# objectsT1 = filterPaths(objects= objects, path= srcFolder)
# print(objects)
# print()

# objectsT1.sort(reverse= True)
# print(objects)
# print()

# newFile = objectsT1[0]
# srcFile = objectsT1[1]
# print(newFile)
# print(type(srcFile))

# s3 = boto3.client('s3')
# BUCKET = os.environ.get('BUCKET_NAME')
# #conn = S3Connection('<access-key>','<secret-access-key>')
# bucket = s3.get_bucket(BUCKET)
# datal = []
# for key in [ x for x in bucket.list() if x.endswith('.csv') ]:
#     print(key)
    #datal.append(pd.read_csv('s3://{bucket}/{key}'.format(bucket=bucket, key=key.name.encode('utf-8'))))

#data = pd.concat(datal)