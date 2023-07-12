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

print(srcFile)
print(newFile)

colName = ['id', 'first_name', 'address']
delimiter = ','

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

    # Rename columns
    diff_src = diff_src.add_prefix('Old_')
    diff_new = diff_new.add_prefix('New_')

    # Rename ID columns to default ('id')
    diff_src.rename(columns = {'Old_' + key_col : key_col}, inplace = True)
    diff_new.rename(columns = {'New_' + key_col : key_col}, inplace = True)

    # Merge
    merged = pd.merge(diff_src, diff_new, on= key_col)

    # Reorder and return
        # Alternate columns
    merged = merged[list(sum(zip(diff_src.columns, diff_new.columns), ()))]

        # Remove duplicate columns
    merged = merged.T.drop_duplicates().T

        # bring key_col to front and return
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
        merged = merge_from_difference_df('id', diff_in_src, diff_in_new)
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