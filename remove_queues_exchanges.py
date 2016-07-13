#script to remove exchanges and queues for virtual host
#python remove_queue_exchanges.py 192.168.1.94 %2f test test
#!/usr/bin/python
__author__ = 'gabriele'

import urllib2
import base64
import time
import datetime
import json
import sys


def print_time(step):
    ts = time.time();
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S');
    print st + " - " + step


def get_auth(user, password):
    return base64.encodestring('%s:%s' % (user, user)).replace('\n', '')


def call_api(rabbitmq_host, vhost, user, password, api):
    request = urllib2.Request("http://" + rabbitmq_host + ":15672/api/" + api);
    request.add_header("Authorization", "Basic %s" % get_auth(user, password))

    request.get_method = lambda: 'GET'
    response = urllib2.urlopen(request)
    print_time(" *** response done, loading json")
    items = json.load(response)
    for item in items:
        print_time(" *** removing " + api + " - " +  item['name'])

        if item['name'].startswith('amq.') or item['name']=="" : 
            print_time(" *** skipped " + item['name'])
            continue
        url_delete = "http://" + rabbitmq_host + ":15672/api/" + api +"/" + item['vhost'] + "/" + item[
                'name']
        print url_delete       
        request_del = urllib2.Request(url_delete)
        request_del.add_header("Authorization",
                               "Basic %s" % get_auth(user, password))
        request_del.get_method = lambda: 'DELETE'
        urllib2.urlopen(request_del)
        print_time(" *** removed " + api + " - " +  item['name'])




if __name__ == '__main__':
    print 'Argument List:', str(sys.argv)
    rabbitmq_host = sys.argv[1];
    call_api(rabbitmq_host, sys.argv[2], sys.argv[3], sys.argv[4], "queues")
    call_api(rabbitmq_host, sys.argv[2], sys.argv[3], sys.argv[4], "exchanges")

