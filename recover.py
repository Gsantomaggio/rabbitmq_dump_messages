# script to dump all rabbitmq messages
# python recover.py 192.168.1.94 %2f test test
# !/usr/bin/python
__author__ = 'gabriele'

import urllib2
import base64
import time
import datetime
import json
import sys
import pika
import os


def time_tostring():
    ts = time.time();
    return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d--%H_%M_%S');


def print_time(step):
    print time_tostring() + " - " + step


def drain_messages(consume_channel, q, file):
    method_frame, header_frame, body = consume_channel.basic_get(q)
    if method_frame:
        file.write(str(header_frame) + "\n")
        file.write(body + "\n")
        drain_messages(consume_channel, q, file)
        #     # channel.basic_ack(method_frame.delivery_tag)
        # else:
        #     print 'No message returned'


def get_auth(user, password):
    return base64.encodestring('%s:%s' % (user, password)).replace('\n', '')


def call_api(rabbitmq_host, vhost, user, password, api):
    print_time("Calling the API: " + api);
    request = urllib2.Request("http://" + rabbitmq_host + ":15672/api/" + api);
    request.add_header("Authorization", "Basic %s" % get_auth(user, password))
    request.get_method = lambda: 'GET'
    response = urllib2.urlopen(request)
    items = json.load(response)
    return items


if __name__ == '__main__':
    print 'Argument List:', str(sys.argv)
    host = sys.argv[1]
    vhost = sys.argv[2]
    user = sys.argv[3]
    password = sys.argv[4]
    queues = call_api(host, vhost, user, password, "queues")

    credentials = pika.PlainCredentials(user, password)
    connection = pika.BlockingConnection(
            pika.ConnectionParameters(host, 5672, "/", credentials))
    channel = connection.channel()
    dump_dir = "dump_time_" + time_tostring()
    if not os.path.exists(dump_dir):
        os.makedirs(dump_dir)
    for queue in queues:
        print_time("Dump queue:" + queue['name'])
        file = open(dump_dir + "/" + queue['name'], 'w+')
        drain_messages(channel, queue['name'], file)
        file.flush()
        file.close()

    call_api(host, vhost, user, password, "connections")

    raw_input("key to stop")


    def kill():
        channel.stop_consuming()


    connection.add_timeout(0, kill)
    print "Goodbye"
