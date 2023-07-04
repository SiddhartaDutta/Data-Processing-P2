import os
import csv
import boto3
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client('s3')

BUCKET_NAME = os.environ.get("BUCKET_NAME")
OBJECT_NAME = 'diff.csv'

srcFile = os.environ.get("SRC_FILE")
newFile = os.environ.get("NEW_FILE")

colName = ['id', 'first_name', 'address']
delimiter = ','


def main():

    # Read .csv files
    src = pd.read_csv(srcFile, usecols= colName, sep= delimiter)
    new = pd.read_csv(newFile, usecols= colName, sep= delimiter)

    # Get differences
    diff_in_new = new[~new.apply(tuple,1).isin(src.apply(tuple,1))]

    # Write new records into .csv file
    with open('diff.csv', 'a') as diff:
        # Get cols
        cols = diff_in_new.head()

        # Write to file 
            # Create writer
        writer = csv.writer(diff)

            # Write header
        writer.writerow(cols)

            # Write rows
        for row in range(len(diff_in_new.index)):
            writer.writerow(diff_in_new.iloc[row])

    # Upload to AWS
    with open("diff.csv", "rb") as f:
        s3.upload_fileobj(f, BUCKET_NAME, OBJECT_NAME)


# Print changes
def printAllChanges(diff_in_orig, diff_in_new):
    for entry in range(len(diff_in_orig.index)):
        for col in diff_in_orig.columns:
            if diff_in_orig.at[diff_in_orig.index[entry], col] != diff_in_new.at[diff_in_new.index[entry], col]:
                print('id: ' + str(diff_in_orig.at[diff_in_orig.index[entry], 'id']))
                print('old: ' + str(diff_in_orig.at[diff_in_orig.index[entry], col]))
                print('new: ' + str(diff_in_new.at[diff_in_new.index[entry], col]) + '\n')

if __name__ == "__main__":
    main()