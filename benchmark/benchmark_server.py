#!/usr/bin/env python3

import subprocess
import zmq

def main():
    context = zmq.Context(1)
    benchmark_socket = context.socket(zmq.REP)
    benchmark_socket.bind('tcp://*:8000')

    while True:
        command = benchmark_socket.recv_string()
        splits = command.split(':')
        num_threads = int(splits[0])
        num_requests = int(splits[1])

        result = subprocess.run([
            './benchmark',
            ('--numThreads %d' % num_threads),
            ('--numRequests %d', % num_requests)
        ], stdout=subprocess.PIPE)

        if result.returncode == 0:
            output = result.stdout
        else:
            output = result.stdout + '\n' + result.stderr

        benchmark_socket.send_string(output)

if __name__ == '__main__':
    main()
