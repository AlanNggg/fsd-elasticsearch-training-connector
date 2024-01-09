from elasticsearch import Elasticsearch

client = Elasticsearch(
    'https://10.18.2.184:9200', 
    verify_certs=False, 
    ssl_show_warn=False, 
    http_auth=('elastic', 'r4zrSi3AnkDs-dvaxUwM'),
    # sniff_on_start=True,  # sniff before doing anything
    sniff_on_connection_fail=True,  # refresh nodes after a node fails to respond
    sniffer_timeout=60,  # and also every 60 seconds
    retry_on_timeout=True,  
)

client.update_by_query(
    index='test',
    query={
        'term': {
            'id': ''
        }
    }
)