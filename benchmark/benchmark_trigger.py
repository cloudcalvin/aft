#!/usr/bin/env python3

import argparse
import sys

import zmq

def main():
    parser = argparse.ArgumentParser(description='Starts an Aft benchmark.')
    parser.add_argument('-r', '--numRequests', nargs=1, type=int, metavar='R',
                        help='The number of requests to run per thread.',
                        dest='requests', required=True)
    parser.add_argument('-t', '--numThreads', nargs=1, type=int, metavar='T',
                        help='The number of threads per server to run.',
                        dest='threads', required=True)
    parser.add_argument('-rp', '--replicas', nargs=1, type=str, metavar='P',
                        help='A comma-separated list of Aft replicas',
                        required=True)
    parser.add_argument('-tp', '--threadPerServer', nargs=1, type=int,
                        metavar='U', help='The number of benchmark servers' +
                        'per machine to contact', dest='tpm' required=True)

    args = parser.parse_args()

    servers = []
    with open('benchmarks.txt') as f:
        servers.append(f.readline())

    message = ('%d:%d:%s') % (args.threads[0], args.threads[0] *
                              args.requests[0], args.replicas)

    conns = []
    context = zmq.Context(1)
    for server in servers:
        for i in range(args.tpm[0]):
            conn = context.socket(zmq.REQ)
            conn.connect('tcp://%s:%d' % (server, 8000 + i))
            conn.send_string(message)

            conns.append(conn)

    for conn in conns:
        response = conn.recv_string()
        print(response)

    print('Finished!')

if __name__ == '__main__':
    main()
