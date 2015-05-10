"""
The purpose of clases in this file is to manage ec2 worker instances
This class requires boto, which is the Amazon EC2 api.  To install it run

apt-get update
apt-get install -y python-pip python-dev
pip install boto
pip install zerorpc
pip install numpy
pip install paramiko

More info here : http://aws.amazon.com/sdk-for-python/

Your access credentials should be in ~/.aws/credentials
"""
import collections
import os

import boto
import boto.ec2
import uuid
import unittest
from boto.s3.key import Key
from os import listdir
from os.path import isfile, join
import paramiko

REGION_NAME = "us-east-1"
SUBNET_ID = "subnet-600b3f5a" #us east 1 c,
IMAGE_ID = 'ami-0c372164'


class EC2Worker(object):

    def __init__(self,num_workers):
        if (num_workers>2):
            raise Exception("Don't put more than 2 workers because this gets expensive")


class EC2WorkerManager(object):

    def __init__(self):
        """Initialize the ec2 worker with the user's ec2 credentials from ~/.aws/credentials """
        self.s3 = boto.connect_s3()
        self.ec2 = boto.ec2.connect_to_region(REGION_NAME)
        self.bucket_name = "micro-spark-project"
        self.bucket = self.s3.create_bucket(self.bucket_name)
        self.keys={}
        self.workers=[]
        self.worker_number=0

    def auto_deploy_server(self,data_dir=["Data","Data1"],code_dir=".",program="worker.py"):
        code_files=[f for f in listdir(code_dir) if isfile(join(dir,f)) and f.endswith("*.py")]
        for d in data_dir:
            self.put_dir_in_s3(data_dir)
        for c in code_files:
            self.put_file_in_s3(c)
        reserve=self.start_worker()
        self.download_s3_files_on_worker(self.bucket_name)
        self.start_worker_process()

    def start_worker_process(self):
        pass

    def download_s3_files_on_worker(self, bucket_name):
        pass

    def put_dir_in_s3(self,dir,bucket):
        files=[f for f in listdir(dir) if isfile(join(dir,f))]
        for f in files:
            self.put_file_in_s3(f)

    def put_file_in_s3(self,fn):
        s3f=open(fn,"r")
        for key in self.bucket.list():
            if (key.name == fn ):
                self.keys[fn]=key
                return

        k = Key(self.bucket)
        k.key = fn
        k.set_contents_from_file(s3f)
        self.keys[fn]=k
        s3f.close()

    def delete_files_in_s3(self):
        for k in self.keys:
            self.keys[k].delete()
        for key in self.bucket.list():
            key.delete()
        self.s3.delete_bucket(self.bucket_name)

    def copy_all_files_from_s3(self,dir=None):
        for key in self.bucket.list():
            if not(dir):
                key.get_contents_to_filename(key.name)
            else:
                key.get_contents_to_filename(dir+"/"+key.name)

    def get_file_from_s3(self, fn, dest):
        self.keys[fn].get_contents_to_filename(dest)
        return open(dest).readlines()

    def start_worker(self):
        """Starts up an ec2 worker"""
        group=self.make_micro_spark_group()
        print "group",group
        print "Creating instance with security group ",group
        reserve=self.ec2.run_instances(
            IMAGE_ID,
            key_name='microspark',
            instance_type='t2.micro',
            subnet_id=SUBNET_ID,
            security_group_ids=[str(group.id)])

        self.workers.append(reserve)

        print "started instance:"
        print self.workers
        self.worker_number+=1
        return reserve

        pass
    def print_active_worker_info(self):
        reservations = self.ec2.get_all_reservations()
        for res in reservations:
            print reservations
            for inst in res.instances:
                print inst

    def run_command_on_worker(self,instance,cmd):
        pass

    def list_workers(self):
        """Lists all active ec2 workers"""
        active=[]
        reservations = self.ec2.get_all_instances()
        print "instances",reservations
        for reserve in reservations:
            for inst in reserve.instances:
                if (inst.state=='running' or inst.state=='pending'):
                    active.append(inst)
        return active


    def shutdown_all_workers(self):
        for inst in self.list_workers():
            self.ec2.terminate_instances(inst.id)

    def make_micro_spark_group(self):
        groupName="microspark"
        groups = [g for g in self.ec2.get_all_security_groups() if g.name == groupName]
        group = groups[0] if groups else None
        if group:
            return group

        print "Creating group '%s'..."%(groupName)
        group = self.ec2.create_security_group(groupName, "%s security group" % groupName,vpc_id="vpc-de103bbb")
        group.authorize(ip_protocol="tcp",
                        to_port="22",
                        from_port="22",
                        cidr_ip="0.0.0.0/0",
                        src_group=None)
        group.authorize(ip_protocol="tcp",
                        to_port="4242",
                        from_port="4242",
                        cidr_ip="0.0.0.0/0",
                        src_group=None)
        return group



class TestEC2(unittest.TestCase):

    def testWorker(self):
        ec2=EC2WorkerManager()
        ec2.start_worker();
        ec2.print_active_worker_info()
        raw_input("Press Return To ShutDown Active Workers")
        ec2.shutdown_all_workers()

    def testS3(self):
        ec2=EC2WorkerManager()
        fn="test_basic.sh"
        ec2.put_file_in_s3(fn)
        tmp_file = "/tmp/"+fn
        if (os.path.exists(tmp_file)):
            os.remove(tmp_file)
        val= ec2.get_file_from_s3(fn,tmp_file)
        self.assertTrue(len(val)>5)
        ec2.delete_files_in_s3()
        fail=False
        os.remove(tmp_file)
        try:
            val= ec2.get_file_from_s3("test_base.sh")
        except Exception,e:
            fail=True
        self.assertTrue(fail)


if __name__ == '__main__':
    unittest.main()