import argparse
import os
import numpy

import common_module

PERCENTAGE_THRESHOLD = 16.0
TIMEOUT_ROUNDS_FILENAME = 'timeout_rounds_for_ip'
IP_TO_USE_FILENAME = 'ip_to_use'

def process_files(data_dir, best_server_for_ip, output_file):
    probe_round_offset = 0
    server_latencies = dict() # Maps from the IP address to a list of latencies.
    for path, _, files in os.walk(data_dir):
        print 'running path: ' + path
        if len(files) <= 0:
            splitted_path = path.split('/')
            #print splitted_path[len(splitted_path) - 1]
            #print str(splitted_path[len(splitted_path) - 1] in common_module.SERVER_SET)
            #print str(common_module.SERVER_SET)
            if splitted_path[len(splitted_path) - 1] in common_module.SERVER_SET:
                print 'From: ' + path
                if probe_round_offset > 0:
                    output_to_file(server, best_server_for_ip, server_latencies, output_file)
                # A new server, reset all the variables.
                probe_round_offset = 0
                server_latencies = dict() # Maps from the IP address to a list of latencies.
                print 'reseted the data structures.'
            continue

        tokenized_path = path.split('/')
        server = tokenized_path[len(tokenized_path) - 2]
        for filename in files:
            if not filename.startswith(common_module.FILENAME_PREFIX):
                continue
            full_path = os.path.join(path, filename)
            with open(full_path, 'rb') as input_file:
                for raw_line in input_file:
                    line = raw_line.rstrip().split()
                    round_id = int(line[common_module.ROUND_COL_INDEX])
                    ip_address = line[common_module.IP_COL_INDEX]
                    latency = float(line[common_module.LATENCY_COL_INDEX])
                    actual_round = round_id + probe_round_offset
                    if ip_address not in server_latencies:
                        server_latencies[ip_address] = []
                    if actual_round < len(server_latencies[ip_address]):
                        # The round already exists.
                        server_latencies[ip_address][actual_round] = min(server_latencies[ip_address][actual_round], latency)
                    else:
                        for i in range(len(server_latencies[ip_address]), actual_round - 1):
                            server_latencies[ip_address].append(10000)
                        server_latencies[ip_address].append(latency)
        probe_round_offset = actual_round + 1 # The probe offset is the last round.
    output_to_file(server, best_server_for_ip, server_latencies, output_file)

def output_to_file(server, best_server_for_ip, server_ips_latencies, output_file):
    output_timeout = open(TIMEOUT_ROUNDS_FILENAME, 'ab')
    ip_output = open(IP_TO_USE_FILENAME, 'ab')
    for ip, latencies in server_ips_latencies.iteritems():
        best_server = best_server_for_ip[ip]
        if server == best_server[0]:
            timeout_rounds_list = []
            for i in range(0, len(latencies)):
                latency = latencies[i]
                if latency > common_module.TIMEOUT_THRESHOLD:
                    if len(timeout_rounds_list) == 0:
                        output_timeout.write(ip)
                    timeout_rounds_list.append(i)
                    output_timeout.write(' ' + str(i))
            percentage = 100.0 * len(timeout_rounds_list) / len(latencies)
            output_file.write(str(percentage) + '\n')
            if len(timeout_rounds_list) > 0:
                output_timeout.write('\n')
            if percentage < PERCENTAGE_THRESHOLD:
                ip_output.write(ip + '\n')
    output_timeout.close()
    ip_output.close()
    
def generate_output_file(output_file):
    return open(output_file, 'wb')

def close_output_file(output_file):
    output_file.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('data_dir')
    parser.add_argument('median_filename')
    parser.add_argument('output_filename')
    args = parser.parse_args()
    best_server_for_ip = common_module.generate_best_server_for_ip(args.median_filename)
    if os.path.exists(TIMEOUT_ROUNDS_FILENAME):
        os.remove(TIMEOUT_ROUNDS_FILENAME)
    if os.path.exists(IP_TO_USE_FILENAME):
        os.remove(IP_TO_USE_FILENAME)
    output_file = generate_output_file(args.output_filename)
    process_files(args.data_dir, best_server_for_ip, output_file)
    close_output_file(output_file)
