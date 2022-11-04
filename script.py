#!/usr/bin/python

import os
import time
import pipes
import boto3



DB_HOST = 'host'
DB_USER = 'user'
DB_USER_PASSWORD = 'password'
DB_NAME = 'backup/dbnameslist.txt'
BACKUP_PATH = 'backup/dbbackup'

# current DateTime to create the separate backup folder like "2018-08-17-12:34:33".
DATETIME = time.strftime('%Y-%m-%d_%H:%M:%S')
TODAYBACKUPPATH = BACKUP_PATH + '/' + DATETIME

def upload_folder_to_s3(s3bucket, inputDir, s3Path):
    """Function to upload folder in s3"""
    print("Uploading results to s3 initiated...")
    print("Local Source:",inputDir)
    os.system("ls -ltR " + inputDir)

    print("Dest  S3path:",s3Path)

    try:
        for path, subdirs, files in os.walk(inputDir):
            for file in files:
                dest_path = path.replace(inputDir,"")
                __s3file = os.path.normpath(s3Path + '/' + dest_path + '/' + file)
                __local_file = os.path.join(path, file)
                print("upload : ", __local_file, " to Target: ", __s3file, end="")
                s3bucket.upload_file(__local_file, __s3file)
                print(" ...Success")
    except Exception as e:
        print(" ... Failed!! Quitting Upload!!")
        print(e)
        raise e


try:
    os.stat(TODAYBACKUPPATH)
except:
    os.mkdir(TODAYBACKUPPATH)


# Starting database schema backup process.
if DB_NAME:
   in_file = open(DB_NAME,"r")
   flength = len(in_file.readlines())
   in_file.close()
   p = 1
   dbfile = open(DB_NAME,"r")

   while p <= flength:
       db = dbfile.readline()   # reading database name from file
       db = db[:-1]         # deletes extra line
       dumpcmd = "mysqldump -h " + DB_HOST + " -u " + DB_USER + " -p" + DB_USER_PASSWORD + " " + db + " > " + pipes.quote(TODAYBACKUPPATH) + "/" + db + ".sql"
       os.system(dumpcmd)
       gzipcmd = "gzip " + pipes.quote(TODAYBACKUPPATH) + "/" + db + ".sql"
       os.system(gzipcmd)
       p = p + 1
   dbfile.close()
else:
   db = DB_NAME
   dumpcmd = "mysqldump -h " + DB_HOST + " -u " + DB_USER + " -p" + DB_USER_PASSWORD + " " + db + " > " + pipes.quote(TODAYBACKUPPATH) + "/" + db + ".sql"
   os.system(dumpcmd)
   gzipcmd = "gzip " + pipes.quote(TODAYBACKUPPATH) + "/" + db + ".sql"
   os.system(gzipcmd)




s3 = boto3.resource('s3', region_name='us-east-1')
s3bucket = s3.Bucket("s3")


for backup in os.listdir(BACKUP_PATH):
    upload_folder_to_s3(s3bucket, f'{BACKUP_PATH}/{backup}', f'/backup/{backup}')

print ("")
print ("Backup complete")
print ("Your backups have been created in '" + TODAYBACKUPPATH + "' directory and uploaded in s3 bucket")
