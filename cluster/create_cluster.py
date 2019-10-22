#!/usr/bin/env python3

#  Copyright 2019 U.C. Berkeley RISE Lab
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import argparse
import os

import boto3

from add_nodes import add_nodes
import util

ec2_client = boto3.client('ec2', os.getenv('AWS_REGION', 'us-east-1'))


def create_cluster(replica_count, bench_count, cfile, ssh_key, cluster_name,
                   kops_bucket, aws_key_id, aws_key):
    util.run_process(['./create_cluster_object.sh', kops_bucket, ssh_key])

    client, apps_client = util.init_k8s()

    os.system('cp %s aft-config.yml' % cfile)
    kubecfg = os.path.join(os.environ['HOME'], '.kube/config')

    # Wait until the monitoring pod is finished creating to get its IP address
    # and then copy KVS config into the monitoring pod.
    prefix = './'
    print('Creating %d Aft replicas...' % (replica_count))
    add_nodes(client, apps_client, cfile, ['aft'], [replica_count], aws_key_id,
              aws_key, True, prefix)
    util.get_pod_ips(client, 'role=aft')

    print('Adding %d benchmark nodes...' % (bench_count))
    add_nodes(client, apps_client, cfile, ['benchmark'], [bench_count],
              create=True, prefix=prefix)

    print('Finished creating all pods...')

    sg_name = 'nodes.' + cluster_name
    sg = ec2_client.describe_security_groups(
          Filters=[{'Name': 'group-name',
                    'Values': [sg_name]}])['SecurityGroups'][0]

    print('Authorizing ports for Aft replicas...')
    permission = [{
        'FromPort': 7654,
        'IpProtocol': 'tcp',
        'ToPort': 7654,
        'IpRanges': [{
            'CidrIp': '0.0.0.0/0'
        }]
    }]

    ec2_client.authorize_security_group_ingress(GroupId=sg['GroupId'],
                                                IpPermissions=permission)
    print('Finished!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='''Creates a Hydro cluster
                                     using Kubernetes and kops. If no SSH key
                                     is specified, we use the default SSH key
                                     (~/.ssh/id_rsa), and we expect that the
                                     correponding public key has the same path
                                     and ends in .pub.

                                     If no configuration file base is
                                     specified, we use the default
                                     ($HYDRO_HOME/anna/conf/anna-base.yml).''')

    parser.add_argument('-r', '--replicas', nargs=1, type=int, metavar='R',
                        help='The number of Aft replicas to start with ' +
                        '(required)', dest='replicas', required=True)
    parser.add_argument('-b', '--benchmark', nargs='?', type=int, metavar='B',
                        help='The number of benchmark nodes in the cluster ' +
                        '(optional)', dest='benchmark', default=0)
    parser.add_argument('--conf', nargs='?', type=str,
                        help='The configuration file to start the cluster with'
                        + ' (optional)', dest='conf',
                        default='../conf/aft-base.yml')
    parser.add_argument('--ssh-key', nargs='?', type=str,
                        help='The SSH key used to configure and connect to ' +
                        'each node (optional)', dest='sshkey',
                        default=os.path.join(os.environ['HOME'],
                                             '.ssh/id_rsa'))

    cluster_name = util.check_or_get_env_arg('HYDRO_CLUSTER_NAME')
    kops_bucket = util.check_or_get_env_arg('KOPS_STATE_STORE')
    aws_key_id = util.check_or_get_env_arg('AWS_ACCESS_KEY_ID')
    aws_key = util.check_or_get_env_arg('AWS_SECRET_ACCESS_KEY')

    args = parser.parse_args()

    create_cluster(args.replicas[0], args.benchmark, args.conf, args.sshkey,
                   cluster_name, kops_bucket, aws_key_id, aws_key)
