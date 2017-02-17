'''
Global variables
'''
import pprint

# Pretty printer for json
pp = pprint.PrettyPrinter(indent=4)

# Timestamp format to use everywhere
TSP_FORMAT = '%Y-%m-%d %H:%M:%S'

# Headers in requests to JHH servers
HEADERS = {
    'client_id':'09487db62cdc41d0a6fafa57a2cd30f5', 
    'client_secret':'7e415e173a7149029606B508289D4799'
}

# All the servers provided by JHH
SERVERS = {
    'test': 'https://api-test.jh.edu/internal/v2/clinical',
    'stage': 'https://api-stage.jh.edu/internal/v2/clinical',
    'prod': 'https://api.jh.edu/internal/v2/clinical',
}
