import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import json

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)



def list_of_files(client, Bucketname,source_folder):
    response = client.list_objects_v2(
        Bucket = Bucketname,
        Prefix = source_folder
    )
    print(response)
    files = []
    if 'Contents' in response:
        for obj in response['Contents']:
            Key = obj['Key']
            if Key != source_folder:
                files.append(Key)
    return files

def copy_from_landing_zone(file, client, Bucketname, copySource,destination_key):
    copy_files = []
    
    response = client.copy_object(
        Bucket = Bucketname,
        CopySource = copySource,
        Key = destination_key
    )
    copy_files.append(file)
    return copy_files

def delete_from_source(client, Bucketname,file):
    response = client.delete_object(
        Bucket = Bucketname,
        Key = file
    )

def main():
    client = boto3.client('s3')
    Bucketname = 'glue-to-redshift-pipeline'
    source_folder = 'landing_zone/'
    dest_folder = 'bronz_layer/'
    files = list_of_files(client,Bucketname,source_folder)

    for file in files:
        file_name = file.split('/')[-1]
        destination_key = dest_folder + file_name
        copySource = {
            'Bucket' : Bucketname,
            'Key' : file
        }
        copy_from_landing_zone(file, client, Bucketname,copySource,destination_key)

        delete_from_source(client, Bucketname,file)


if __name__ =='__main__':
    main()


job.commit()