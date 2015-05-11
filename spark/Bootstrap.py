"""
This file is the only file that is put on the AMI image.

It is called via

python Bootstrap.py <s3 bucket> <aws_access> <aws secret> <program> <program arg>

Credentials should be in the ~/.aws/credentials directory on that machine
"""
from os import system
import os
import boto
import sys
import socket


class Bootstrap(object):

    def __init__(self,bucket):
        self.bucket=bucket

    def copy_files_and_run(self,port):
        self.s3 = boto.connect_s3()
        bucket=self.s3.get_bucket(self.bucket)
        for key in bucket.list():
            if (len(os.path.dirname(key.name))>0):
                print key.name
                print os.path.dirname(key.name)
                if (not(os.path.exists(os.path.dirname(key.name)))):
                    os.mkdir(os.path.dirname(key.name))
            key.get_contents_to_filename(key.name)

def usage():
    print "Usage: "+"Bootstrap.py <s3 bucket> <port>"


if __name__ == '__main__':
    print "v.01 args were "+str(sys.argv)
    if (len(sys.argv)<2):
        usage()
        exit()

    b=Bootstrap(sys.argv[1])
    b.copy_files_and_run(sys.argv[2])