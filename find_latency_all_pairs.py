import argparse
import os
import numpy

import common_module

def process_files(data_dir, percentiles, output_files, ip_to_use=None, replace_timeout=False, cloud=None):
    probe_round_offset = 0
    server_latencies = dict() # Maps from the IP address to a list of latencies.
    for path, _, files in os.walk(data_dir):
        print 'running path: ' + path
        if len(files) <= 0:
            splitted_path = path.split('/')
            if splitted_path[len(splitted_path) - 1] in common_module.SERVER_SET:
                print 'From: ' + path
                if probe_round_offset > 0 and common_module.check_cloud(cloud, server):
                    output_to_file(server, server_latencies, percentiles, output_files, ip_to_use=ip_to_use, replace_timeout=replace_timeout)
                # A new server, reset all the variables.
                probe_round_offset = 0
                server_latencies = dict() # Maps from the IP address to a list of latencies.
                print 'reseted the data structures.'
            continue

        tokenized_path = path.split('/')
        server = tokenized_path[len(tokenized_path) - 2]
        if common_module.check_cloud(cloud, server):
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
    if common_module.check_cloud(cloud, server):
        output_to_file(server, server_latencies, percentiles, output_files, ip_to_use=ip_to_use, replace_timeout=replace_timeout)

def output_to_file(server, server_ips_latencies, percentiles, output_files, ip_to_use=None, replace_timeout=False):
    output_timeout = open('timeout_dns_servers.txt', 'ab')
    for ip, latencies in server_ips_latencies.iteritems():
        if replace_timeout:
            for i in range(0, len(latencies)):
                latency = latencies[i]
                if latency > common_module.TIMEOUT_THRESHOLD:
                    if i == 0:
                        latencies[i] = latencies[i + 1]
                    elif i == len(latencies) - 1:
                        latencies[i] = latencies[i - 1]
                    else:
                        latencies[i] = min(latencies[i - 1], latencies[i + 1])
        if ip_to_use is None or ip in ip_to_use:
            for i in range(0, len(percentiles)):
                result = numpy.percentile(latencies, int(percentiles[i]))
                if result > common_module.TIMEOUT_THRESHOLD and int(percentiles[i]) == 99:
                    output_timeout.write(ip + ' ' + str(result) + '\n')
                elif result <= common_module.TIMEOUT_THRESHOLD:
                    output_files[i].write(server + ' ' + ip + ' ' + str(result) + '\n')
    output_timeout.close()
    
def generate_output_files(percentiles, output_dir):
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    retval = []
    for percentile in percentiles:
        filename = str(percentile) + '_percentile_all_pairs.txt'
        full_path = os.path.join(output_dir, filename)
        retval.append(open(full_path, 'wb'))
    return retval

def parse_ip_to_use(ip_to_use_file):
    if ip_to_use_file is None:
        return ip_to_use_file
    retval = set()
    with open(ip_to_use_file, 'rb') as input_file:
        for line in input_file:
            retval.add(line.rstrip())
    return retval

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('data_dir')
    parser.add_argument('percentiles', nargs='*')
    parser.add_argument('output_dir')
    parser.add_argument('--ip-to-use', default=None)
    parser.add_argument('--ignore-timeout')
    parser.add_argument('--cloud', default=None)
    args = parser.parse_args()
    ip_to_use = parse_ip_to_use(args.ip_to_use)
    output_files = generate_output_files(args.percentiles, args.output_dir)
    process_files(args.data_dir, args.percentiles, output_files, cloud=args.cloud)
