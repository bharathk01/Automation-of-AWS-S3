import glob 
import boto3 
import os 
import sys 
import json
from multiprocessing.pool import ThreadPool 


#create a s3 bucket 
s3 = boto3.resource("s3")

print("If you want to create a new bucket the name of bucket should not equal with the names of the buckets in your s3 of AWS a/c \n\n")
bucket_name = input("\n\nEnter the bucket name to be created and uploaded (with public access): ") 

bucket = s3.Bucket(bucket_name)
region = input("Enter your location constraint: ")
response = bucket.create(
    ACL='public-read',
    CreateBucketConfiguration={
    'LocationConstraint': '%s' %region
    },
 )
    
print("\nBucket created...\n\n")




# Create an S3 client
session = boto3.Session(profile_name='default')

s3= boto3.client('s3')


# Create the bucket policy with all public access

bucket_policy={
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "PublicRead",
      "Action": "s3:*",
      "Effect": "Allow",
      "Resource": "arn:aws:s3:::%s/*" %bucket_name,
      "Principal": "*"
    }
  ]
}
    
# Convert the policy to a JSON string
bucket_policy = json.dumps(bucket_policy)

# Set the new policy on the given bucket
s3.put_bucket_policy(Bucket=bucket_name, Policy=bucket_policy)

print("Done....Public policy created\n\n")





# target location of the files on S3  

S3_FOLDER_NAME = input("Enter the folder name to be created on s3 bucket: ")

# Enter your own credentials file profile name below
#AWS_PROFILE="default"


# Source location of files on local system 

DATA_FILES_LOCATION   = input("\nEnter the path of all files to be Upload ends with [\*.filetype]: ") 

session = boto3.Session(profile_name='default') 

#s3 = session.client('s3') 

# The list of files we're uploading to S3 

filenames =  glob.glob(DATA_FILES_LOCATION) 


def upload(myfile): 

    s3_file = f"{S3_FOLDER_NAME}/{os.path.basename(myfile)}" 

    s3.upload_file(myfile, bucket_name, s3_file) 


# Number of pool processes is a guestimate - I've set 
# it to twice number of files to be processed 

pool = ThreadPool(processes=len(filenames)*2) 


pool.map(upload, filenames) 


print("\n\nAll Data files uploaded to S3....\n\n") 
