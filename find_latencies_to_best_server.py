import argparse

import common_module

def process_file(best_server_for_ip, latencies_file):
    retval = []
    with open(latencies_file, 'rb') as input_file:
        for raw_line in input_file:
            line = raw_line.rstrip().split()
            ip = line[1]
            server = line[0]
            if server == best_server_for_ip[ip][0]:
                retval.append(float(line[2]))
    retval.sort()
    return retval

def output_to_file(result, output_filename):
    with open(output_filename, 'wb') as output_file:
        for latency in result:
            output_file.write(str(latency) + '\n')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('latencies_file')
    parser.add_argument('median_latency_file')
    parser.add_argument('output_filename')
    parser.add_argument('--set-cover', default=None)
    args = parser.parse_args()
    set_cover = common_module.parse_set_cover(args.set_cover)
    print 'set_cover: ' + str(set_cover)
    best_server_for_ip = common_module.generate_best_server_for_ip(args.median_latency_file, set_cover=set_cover)
    result = process_file(best_server_for_ip, args.latencies_file)
    output_to_file(result, args.output_filename)
