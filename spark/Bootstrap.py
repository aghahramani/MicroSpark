"""
This file is the only file that is put on the AMI image.

It is called via

python Bootstrap.py <s3 bucket> <aws_access> <aws secret> <program> <program arg>

Credentials should be in the ~/.aws/credentials directory on that machine
"""
from os import system
import boto
import sys
import socket


class Bootstrap(object):

    def __init__(self,bucket,aws_access,aws_secret):
        self.bucket=bucket
        self.aws_access=aws_access
        self.aws_secret=aws_secret

    def copy_files_and_run(self,port):
        self.s3 = boto.connect_s3(self.aws_access,self.aws_secret)
        bucket=self.s3.get_bucket(self.bucket)
        for key in bucket.list():
            key.get_contents_to_filename(key.name)
        system("./worker.py --ec2 " +str(port) + " &" )
        print(socket.gethostname(),socket.dom)

def usage():
    print "Usage: "+"Bootstrap.py <s3 bucket> <aws_access> <aws secret> <port>"


if __name__ == '__main__':
    if (len(sys.argv)<4):
        usage()
        exit()

    b=Bootstrap(sys.argv[1],sys.argv[2],sys.argv[3])
    b.copy_files_and_run(sys.argv[4])