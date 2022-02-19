#This program wraps the DSBULK load utility to load data into cassandra cluster
#Requires python 3.8 or above
import argparse
import subprocess


class BulkLoad():
    def __init__(self,exe="dsbulk"):
        self.module_name = "Bulk load module"
        self.exe = exe

    def format_command(self,usr,pwd,config):
        cmd = [self.exe,'load','-u',usr,'-p',pwd,'-f',config]
        return cmd

    def execute_dsbulk(self,cmd):
        cmd_result = subprocess.run(cmd,
                                capture_output=True,
                                text=True,
                                check=True,
                                shell=True
                                )
        if cmd_result.returncode !=0:
            raise subprocess.CalledProcessError(
                returncode=cmd_result.returncode,
                cmd=cmd_result.args,
                stderr=cmd_result.stderr
            )
        else:
            print('-----------------------------------------------------')
            print("Command Result: {result}".format(result=cmd_result))

if __name__=="__main__":
    parser = argparse.ArgumentParser(description="This program does bulk loading")
    parser.add_argument('-u', '--user',
                        help='Specify the username to connect to Cassandra Cluster',
                        required=True)
    parser.add_argument('-p','--password',
                        help='Specify the password to connect to Cassandra Cluster',
                        required=True)
    parser.add_argument('-f', '--file',
                        help='Specify the name of the configuration file',
                        required=True)
    parser.add_argument('-e','--exe',
                        help='Specify the location of the DSBULK exe',
                        default='dsbulk')

    parameters=parser.parse_args()
    user = parameters.user
    password = parameters.password
    file = parameters.file
    exe = parameters.exe

    bulk_load = BulkLoad(exe)
    cmd=bulk_load.format_command(user,password,file)
    bulk_load.execute_dsbulk(cmd)
    
