# Column index in the files.
ROUND_COL_INDEX = 1
IP_COL_INDEX = 4
LATENCY_COL_INDEX = 6

SERVER_SET = ['azure_brazil', 'azure_centralus', 'azure_eastasia', 'azure_eastus', \
            'azure_northeurope', 'azure_southeastasia', 'azure_westeurope', 'azure_westus', \
            'ec2_california', 'ec2_frankfurt', 'ec2_ireland', 'ec2_oregon', 'ec2_saopaulo', \
            'ec2_singapore', 'ec2_sydney', 'ec2_tokyo', 'ec2_useast', 'google_asia', 'google_europe', \
            'google_us']

AZURE = SERVER_SET[0:8]
EC2 = SERVER_SET[8:17]
GOOGLE = SERVER_SET[17:20]

FILENAME_PREFIX = 'probe_'

TIMEOUT_THRESHOLD = 2950.0

def generate_best_server_for_ip(median_filename, set_cover=None):
    retval = dict()
    with open(median_filename) as input_file:
        for raw_line in input_file:
            line = raw_line.rstrip().split()
            ip = line[1]
            server = line[0]
            if set_cover is None or (set_cover is not None and server in set_cover):
                latency = float(line[2])
                if ip not in retval:
                    retval[ip] = (server, latency)
                else:
                    if retval[ip][1] > latency:
                        retval[ip] = (server, latency)
    return retval

# Returns a set of servers.
def parse_set_cover(set_cover_filename=None):
    if set_cover_filename is None:
        return None
    retval = set()
    with open(set_cover_filename) as input_file:
        for raw_line in input_file:
            retval.add(raw_line.rstrip())
    return retval

def parse_latencies(filename):
    retval = dict()
    with open(filename, 'rb') as input_file:
        for raw_line in input_file:
            line = raw_line.rstrip().split()
            server = line[0]
            ip = line[1]
            latency = float(line[2])
            retval[(server, ip)] = latency
    return retval

def check_cloud(cloud, server):
    return cloud is None or (cloud == 'azure' and server in AZURE) or \
                    (cloud == 'ec2' and server in EC2)
