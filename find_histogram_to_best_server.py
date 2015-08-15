import argparse

import common_module

def process_file(median_filename, best_server_to_ip):
    histogram = dict()
    with open(median_filename, 'rb') as input_file:
        for raw_line in input_file:
            line = raw_line.rstrip().split()
            server = line[0]
            ip = line[1]
            latency = float(line[2])
            best_server = best_server_to_ip[ip][0]
            if server == best_server:
                if server not in histogram:
                    histogram[server] = []
                histogram[server].append(ip)
    for server in histogram:
        print 'Server: ' + server + ' #nameservers: ' + str(len(histogram[server]))
 
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('median_filename')
    args = parser.parse_args()
    best_server_to_ip = common_module.generate_best_server_for_ip(args.median_filename)
    process_file(args.median_filename, best_server_to_ip)
    
