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

# MAIN
def main():

    # Read full-> Create and compare short 
    try:
        srcFull = pd.read_csv(srcFile, sep= delimiter)
        src = srcFull[colName]
    except:
        print('Missing src file')
        sendEmail(sns_client, 'Daily Check: Failed', 'Daily check could not be completed: missing \'src.csv\' file.', TOPIC_ARN)
        exit()
        
    try:
        newFull = pd.read_csv(newFile, sep= delimiter)
    except:
        print('Missing new file')
        sendEmail(sns_client, 'Daily Check: Failed', 'Daily check could not be completed: missing \'new.csv\' file.', TOPIC_ARN)
        exit()

    # Get differences
    diff_in_new = newFull[colName][~newFull[colName].apply(tuple,1).isin(src.apply(tuple,1))]

    # Cut down new and old
    srcFull = srcFull[srcFull['id'].isin(diff_in_new['id'])]
    newFull = newFull[newFull['id'].isin(diff_in_new['id'])]

    # Rename columns
    srcFull = srcFull.add_prefix('Old_')
    newFull = newFull.add_prefix('New_')

    # Rename ID cols to default
    srcFull.columns.values[0] = 'id'
    newFull.columns.values[0] = 'id'

    # Merge src > new
    newFull = pd.merge(srcFull, newFull, on=['id'])
    
    if len(diff_in_new):

        # Write new records into .csv file
        newFull.to_csv('diff.csv', index= False)
    
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