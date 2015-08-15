# Outputs servers that is the cover set of the nameservers.
import argparse

import common_module

# Finds the cover set.
def find_server_cover_set(median_latencies, server_set, nameserver_set, threshold, output_filename, cloud=None):
    prev_cover_set = None
    current_cover_set = set()
    total_nameservers = len(nameserver_set)
    while current_cover_set != prev_cover_set and \
            len(server_set) > 0 and \
            len(nameserver_set) > 0:
        prev_cover_set = set(current_cover_set)
        histogram = dict() # mapping from server --> list of pl nodes that are < threshold
        for pair, latency in median_latencies.iteritems():
            server, nameserver = pair
            if server in server_set and nameserver in nameserver_set:
                # Only consider unchosen server and unchosen planetlab nodes
                if common_module.check_cloud(cloud, server):
                    if server not in histogram:
                        histogram[server] = set()
                    if latency < threshold:
                        histogram[server].add(nameserver)
        
        # Update only if the histogram contains some elements.
        if len(histogram) > 0:
            best_server = None
            num_nodes_covered = -1
            for server in histogram:
               if len(histogram[server]) > num_nodes_covered:
                   best_server = server
                   num_nodes_covered = len(histogram[server])
            print 'best server: ' + best_server + ' num nodes covered: ' + str(num_nodes_covered)
            percentage = num_nodes_covered * 100.0 / total_nameservers
            if num_nodes_covered > 0 and percentage > 1:
                # At this point, we get the best_server
                # Remove that server and remove the planetlab nodes covered.
                server_set.remove(best_server)
                nodes_covered = histogram[best_server]
                for node in nodes_covered:
                    nameserver_set.remove(node)
                current_cover_set.add(best_server)
    with open(output_filename, 'wb') as output_file:
        # Done finding the cover set
        for server in current_cover_set:
            output_file.write(str(server) + '\n')
    print 'cover set: ' + str(current_cover_set) + ' remaining nameservers: ' + str(len(nameserver_set)) + ' remaining servers: ' + str(len(server_set))

# Returns a set of server and set of planetlab nodes.
def get_servers(median_latencies, cloud=None):
    print cloud
    server_set = set()
    nameserver_set = set()
    for pair, latency in median_latencies.iteritems():
        nameserver_set.add(pair[1])
        if common_module.check_cloud(cloud, pair[0]):
            server_set.add(pair[0])
    print '# Server: ' + str(len(server_set)) + ' # Nameserver: ' + str(len(nameserver_set))
    return server_set, nameserver_set

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('median_filename')
    parser.add_argument('threshold')
    parser.add_argument('output_filename')
    parser.add_argument('--cloud', default=None)
    args = parser.parse_args()
    median_latencies = common_module.parse_latencies(args.median_filename)
    server_set, nameserver_set = get_servers(median_latencies, args.cloud)
    find_server_cover_set(median_latencies, server_set, nameserver_set, float(args.threshold), args.output_filename, args.cloud)

