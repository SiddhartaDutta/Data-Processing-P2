###
#
# Compare last 2 days of inventory/product records (AWS S3 Storage, Glue Job)
# BASE SCRIPT
# 
###

import os
import csv
import boto3
import logging
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client('s3')
sns_client = boto3.client('sns')

# test_data > diff folder
# test_upload > incoming data

BUCKET_NAME = os.environ.get('BUCKET_NAME')
OBJECT_NAME = 'diff.csv'

TOPIC_ARN = os.environ.get('TOPIC_ARN')

srcFile = 's3://' + BUCKET_NAME + '/test_upload/src.csv'
newFile = 's3://' + BUCKET_NAME + '/test_upload/new.csv'

srcFile = 'src.csv'
newFile = 'new.csv'

srcFile = 'RBH_ProductMaster_20230711_0730_DATA.txt'
newFile = 'RBH_ProductMaster_20230712_0730_DATA.txt'

print(srcFile)
print(newFile)

colName = ['id', 'first_name', 'address']
colName = ['Local_Product_Code','Product_Long_Name_PL','Product_Base_UOM_Name','Brand_Product_Per_Pack_Qty', 'Product_Tar_Qty']
delimiter = '|'

# Print changes
def printAllChanges(diff_in_orig, diff_in_new):
    for entry in range(len(diff_in_orig.index)):
        for col in diff_in_orig.columns:
            if diff_in_orig.at[diff_in_orig.index[entry], col] != diff_in_new.at[diff_in_new.index[entry], col]:
                print('id: ' + str(diff_in_orig.at[diff_in_orig.index[entry], 'id']))
                print('old: ' + str(diff_in_orig.at[diff_in_orig.index[entry], col]))
                print('new: ' + str(diff_in_new.at[diff_in_new.index[entry], col]) + '\n')

# Send email
def sendEmail(snsClient, subject, message, topicArn):
    snsClient.publish(TopicArn= topicArn,
                   Message= message,
                   Subject = subject)

# Create pre-assigned URL
def createPreassignedURL(s3Client, bucket, object, expiration):
    try:
        response = s3Client.generate_presigned_url('get_object',
                                                    Params={'Bucket': bucket,
                                                            'Key': object},
                                                    ExpiresIn=expiration)

    except ClientError as e:
        logging.error(e)
        return None

    return response

def merge_from_difference_df(key_col, diff_src, diff_new):
    """Pass in 2 difference dataframes, one listing changes with base src and the other base new. Returns merged dataframe."""

    # Merge
    merged = diff_new.join(diff_src, lsuffix='_new', rsuffix='_old', how='left')

    # Rename old columns
    for col in range(len(diff_new.columns)):
        diff_new.columns.values[col] = diff_new.columns[col] + '_new'
        diff_src.columns.values[col] = diff_src.columns[col] + '_old'

    # Alternate columns
    merged = merged[list(sum(zip(diff_new.columns, diff_src.columns), ()))]
    #print(merged1.columns)
    #merged = merged1[ ['Local_Product_Code_new'] + [ col for col in merged1.columns if col != 'Local_Product_Code_new']]
    #print(merged)

    # Drop duplicate key_col
    merged = merged.drop(key_col + '_old', axis=1)
    
    # Rename key_col
    merged = merged.rename(columns={key_col + '_new' : key_col})

    # Bring key_col to front and return
    cols = list(merged)
    cols.insert(0, cols.pop(cols.index(key_col)))
    return merged.loc[:, cols]


# MAIN
def main():

    # Read full-> Create and compare short 
    try:
        src = pd.read_csv(srcFile, usecols=colName, sep= delimiter)
        #src = srcFull[colName]
    except:
        print('Missing src file')
        sendEmail(sns_client, 'Daily Check: Failed', 'Daily check could not be completed: missing \'src.csv\' file.', TOPIC_ARN)
        exit()
        
    try:
        new = pd.read_csv(newFile, usecols=colName, sep= delimiter)
        #new = newFull[colName]
    except:
        print('Missing new file')
        sendEmail(sns_client, 'Daily Check: Failed', 'Daily check could not be completed: missing \'new.csv\' file.', TOPIC_ARN)
        exit()

    # Get differences
    diff_in_new = new[~new.apply(tuple,1).isin(src.apply(tuple,1))]
    diff_in_src = src[~src.apply(tuple,1).isin(new.apply(tuple,1))]

    
    if len(diff_in_new):

        # Write new records into .csv file
        merged = merge_from_difference_df('Local_Product_Code', diff_in_src, diff_in_new)
        print(merged)
        merged.to_csv('diff.csv', index= False)
    
        # Upload to AWS
        with open("diff.csv", "rb") as f:
            s3.upload_fileobj(f, BUCKET_NAME, 'test_data/' + OBJECT_NAME)
            
        print('Changes Found')
        sendEmail(sns_client, 'Daily Check',
                    'Difference found between daily files. diff.csv uploaded to \'test_data\'. Download link: ' + createPreassignedURL(s3, BUCKET_NAME, 'test_data/' + OBJECT_NAME, 3600),
                    TOPIC_ARN)
        
    else:
        print('No Changes Found')
        sendEmail(sns_client, 'Daily Check', 'No difference found between files; no uploaded difference file.', TOPIC_ARN)

if __name__ == "__main__":
    main()