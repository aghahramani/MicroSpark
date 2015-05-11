"""
The purpose of clases in this file is to manage ec2 worker instances
This class requires boto, which is the Amazon EC2 api.  To install it run

apt-get update
apt-get install -y python-pip python-dev
pip install boto
pip install zerorpc
pip install numpy
pip install paramiko
pip install scp
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
import gevent
import paramiko
import select
import rdd
from driver import WorkerQueue
import zerorpc
from scp import SCPClient

KEY_FILE = "microspark.pem"
REGION_NAME = "us-east-1"
SUBNET_ID = "subnet-600b3f5a" #us east 1 c,
#IMAGE_ID = 'ami-0c372164'
IMAGE_ID= 'ami-8a6a7be2'
FILES_BUCKET = 'micro-spark-project'

def p(tag,str):
    print tag,str

class EC2MicroSparkNode(object):

    def __init__(self,instance):
        self.instance=instance
        self.ip=instance.private_ip_address
        self.wait_for_vm_to_be_ready()
        self.ports=[]

    def wait_for_vm_to_be_ready(self):
        done=False
        pending=False
        while (done==False):
            done=True
            i=self.instance
            if (i.state=='pending'):
                p("Status","Waiting for Node to Come Up, Usually takes about 15 seconds");
                gevent.sleep(5)
                pending=True
                i.update()
                done=False
            elif (i.state=='running'):
                p("Status","Node is up")
                if pending:
                    #Sometimes these take a while to come up
                    gevent.sleep(30)
                    self.bootstrap()
                break
            else:
                raise "Invalid Status "+i.status

    def create_ssh(self):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        k = open(KEY_FILE, "r")
        mykey = paramiko.RSAKey.from_private_key(k)
        k.close()
        ssh.connect(self.ip, username="ubuntu", pkey=mykey)
        return ssh

    def exec_ssh_command(self, cmd):
        stdin, stdout, stderr = self.ssh.exec_command(cmd);
        while not stdout.channel.exit_status_ready():
            if stdout.channel.recv_ready():
                rl, wl, xl = select.select([stdout.channel], [], [], 0.0)
                if len(rl) > 0:
                    print stdout.channel.recv(1024),

    def bootstrap(self):
        """ Copy credentials and Bootstrap.py to server and call Bootstrap.py to download deployment bucket """
        p("Bootstrap",self.ip)
        self.ssh = self.create_ssh()

        scp = SCPClient(self.ssh.get_transport())
        scp.put("microspark-aws-credentials","/home/ubuntu/.aws/credentials")
        scp.close()

        scp = SCPClient(self.ssh.get_transport())
        scp.put("microspark-aws-credentials","/home/ubuntu/.boto")
        scp.close()


        scp = SCPClient(self.ssh.get_transport())
        scp.put("Bootstrap.py","/home/ubuntu/microspark/spark/Bootstrap.py")
        scp.close()
        cmd = "cd microspark/spark; python ./Bootstrap.py "+FILES_BUCKET
        self.exec_ssh_command(cmd)
#        cmd = "killall -9 python"
#        self.exec_ssh_command(cmd)

    def url(self,port):
        return "tcp://"+self.ip+":"+str(port)

    def start_worker(self,port):
        self.ports.append(port)
        p("Starting Worker",self.url(port))
        cmd = "cd microspark/spark; nohup python ./worker.py --master "+rdd.RDD.master+" "+str(port)+"&"
        print "running "+cmd
        self.exec_ssh_command(cmd)


class EC2Worker(WorkerQueue):

    def __init__(self,num_workers=2):
        if (num_workers>2):
            raise Exception("Don't put more than 2 workers because this gets expensive")
        self.manager=EC2WorkerManager()
        self.vms=[ EC2MicroSparkNode(n) for n in self.manager.list_workers()]
        if (len(self.vms)>num_workers+1):
            # We wil have to restart nodes but need to Prevent Spending Too Much Money By Accident
            self.manager.shutdown_all_workers()
            self.vms=[]
        self.portmap={}
        super(EC2Worker,self).__init__()

    def connect(self,value):
        c = zerorpc.Client()
        value=self.portmap[value]
        print "connecting to "+str(value)
        c.connect(value)
        #c.connect("tcp://172.30.0.137:"+str(value))
        return c

    def start_server(self,m):
        s = zerorpc.Server(m)
        s.bind("tcp://0.0.0.0:4241")
        s.run()

    def start_worker(self,port):
        if (len(self.vms)<2):
            self.manager.copy_deployment_to_s3()
            vm=EC2MicroSparkNode(self.manager.start_worker().instances[0])
            p("Started vm",vm)
            self.vms.append(vm)

        #p("VMS",self.vms)
        # this doesn't work yet #
        vmpick= port%len(self.vms)
        #Setting it to one machine or another works though
        #vmpick=0
        #vmpick=1
        self.vms[vmpick].start_worker(port)
        self.portmap[port]=self.vms[vmpick].url(port)
        return port


    def create_connection(self,value):
        c = zerorpc.Client(timeout=5)
        print "connecting to "+str(value)
        value=self.portmap[value]
        c.connect(value)
        #c.connect("tcp://172.30.0.137:"+str(value))
        return c


class EC2WorkerManager(object):

    def __init__(self):
        """Initialize the ec2 worker with the user's ec2 credentials from ~/.aws/credentials """
        self.s3 = boto.connect_s3()
        self.ec2 = boto.ec2.connect_to_region(REGION_NAME)
        self.bucket_name = FILES_BUCKET
        self.bucket = self.s3.create_bucket(self.bucket_name)
        self.keys={}
        self.workers=[]
        self.worker_number=0
#Uncomment to redeploy
#        self.delete_files_in_s3();
        self.copy_deployment_to_s3()


    def copy_deployment_to_s3(self,data_dir=["Data","Data1","Data2"],code_dir=".",program="worker.py"):
        code_files=[f for f in listdir(code_dir) if isfile(join(code_dir,f)) and f.endswith("py") and not f.endswith("Bootstrap.py")]
        p("code_files ",code_files)
        for d in data_dir:
            self.put_dir_in_s3(data_dir,FILES_BUCKET)
        for c in code_files:
            self.put_file_in_s3(c)

    def start_worker_process(self):
        pass

    def download_s3_files_on_worker(self, bucket_name):
        pass

    def put_dir_in_s3(self,dir,bucket):
        for d in dir:
            files=[f for f in listdir(d) if isfile(join(d,f))]
            for f in files:
                self.put_file_in_s3(join(d,f))

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
                        to_port="4000",
                        from_port="5000",
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
