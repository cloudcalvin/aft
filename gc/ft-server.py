#!/usr/bin/env python3

import logging
import os
import time

import boto3
import kubernetes as k8s

ec2 = boto3.client('ec2')

logging.basicConfig(filename='log_ft.txt', level=logging.INFO,
                    format='%(asctime)s %(message)s')

def main():
    while not os.path.isfile('../replicas.txt'):
        continue # Wait till the file is copied in.

    with open('../replicas.txt', 'r') as f:
        replicas = list(map(lambda line:  line.strip(), f.readlines()))

    cfg = k8s.config
    cfg.load_kube_config()
    client = k8s.client.CoreV1Api()

    while True:
        time.sleep(5)
        logging.info("Checking for errors...")

        # List all Aft nodes.
        current_replicas = list_all_aft_ips(client, True)

        if len(current_replicas) < len(replicas):
            logging.info("Found a potential fault: there are fewer replicas"
                         " than expected.")

            # Move a node over from standby.
            sb_nodes = client.list_node(label_selector='role=standby').items
            print('Found some nodes to list...')
            if len(sb_nodes) == 0:
                # wait for more standby nodes to come online
                logging.info("Looping on standby nodes.")
                continue

            node = sb_nodes[0]
            print('Found a node!')
            int_ip = None
            for address in node.status.addresses:
                if address.type == 'InternalIP':
                    int_ip = address.address

            node.metadata.labels['role'] = 'aft'
            client.patch_node(node.metadata.name, node)

            logging.info("Now attempting to change tags...")
            instance_id = ec2.describe_instances(Filters=[{'Name':
                                                           'network-interface.addresses.private-ip-address',
                                                           'Values':
                                                           ['172.20.51.28']}])['Reservations'][0]['Instances'][0]['InstanceId']
            instance = ec2.Instance(instance_id)
            instance.delete_tags(Tags=[{'Key': 'aws:autoscaling:groupName'},
                                       {'Key': 'Name'}])
            instance.create_tags(Tags=[
                {
                    'Key': 'aws:autoscaling:groupName',
                    'Value': 'aft-instances.ucbfluent.de'
                },
                {
                    'Key': 'Name',
                    'Value': 'aft-instances.ucbfluent.de'
                }
            ])
            logging.info("Changed tags...")


            time.sleep(2) # wait for the new pod to come online
            setup_pod(client, int_ip)
        else:
            cr_set = set(current_replicas)
            cr_removed_set = set(cr_set)
            for replica in replicas:
                for cr in cr_set:
                    if cr[0] == replica:
                        cr_removed_set.remove(cr)

            if len(cr_removed_set) > 0:
                for node in cr_set:
                    setup_pod(client, node[1])
            else:
                logging.info("No faults detected.")

        replicas = list_all_aft_ips(client, False)


def list_all_aft_ips(client, include_internal=False):
    nodes = client.list_node(label_selector='role=aft').items
    current_replicas = []
    for node in nodes:
        ext_ip = None
        int_ip = None
        for address in node.status.addresses:
            if address.type == 'ExternalIP':
                ext_ip = address.address
            elif address.type == 'InternalIP':
                int_ip = address.address

        if include_internal:
            current_replicas.append((ext_ip, int_ip))
        else:
            current_replicas.append(ext_ip)

    return current_replicas


def setup_pod(client, internal_ip):
    logging.info('Setting up pod %s...' % internal_ip)
    pods = client.list_namespaced_pod(namespace='default',
                                      label_selector='role=aft').items

    pname = None
    running = False

    logging.info('Waiting for pod to be running...')
    while not running:
        for pod in pods:
            if pod.status.pod_ip == internal_ip:
                pname = pod.metadata.name
                if pod.status.phase == 'Running':
                    running = True # wait till the pod is running
    logging.info('Found pod %s!' % pname)

    eopy_file_to_pod(client, '../conf/aft-config.yml', pname,
                     '/go/src/github.com/vsreekanti/aft/conf', 'aft-container')
    copy_file_to_pod(client, '../replicas.txt', pname,
                     '/go/src/github.com/vsreekanti/aft', 'aft-container')
    logging.info('Copied files to %s...')

# from https://github.com/aogier/k8s-client-python/
# commmit: 12f1443895e80ee24d689c419b5642de96c58cc8/
# file: examples/exec.py line 101
def copy_file_to_pod(client, file_path, pod_name, pod_path, container):
    exec_command = ['tar', 'xmvf', '-', '-C', pod_path]
    resp = stream(client.connect_get_namespaced_pod_exec, pod_name, NAMESPACE,
                  command=exec_command,
                  stderr=True, stdin=True,
                  stdout=True, tty=False,
                  _preload_content=False, container=container)

    filename = file_path.split('/')[-1]
    with TemporaryFile() as tar_buffer:
        with tarfile.open(fileobj=tar_buffer, mode='w') as tar:
            tar.add(file_path, arcname=filename)

        tar_buffer.seek(0)
        commands = [str(tar_buffer.read(), 'utf-8')]

        while resp.is_open():
            resp.update(timeout=1)
            if resp.peek_stdout():
                pass
            if resp.peek_stderr():
                logging.info("Unexpected error while copying files: %s" %
                      (resp.read_stderr()))
                sys.exit(1)
            if commands:
                c = commands.pop(0)
                resp.write_stdin(c)
            else:
                break
        resp.close()


if __name__ == '__main__':
    main()
