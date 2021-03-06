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
import sqlite3
import time
import urllib


   


def time_tostring():
    ts = time.time();
    return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d--%H_%M_%S');


def print_time(step):
    print time_tostring() + " - " + step


def write_message_to_file(conn, queue, method_frame,  header_frame, body):
    conn.execute('insert into dump values (?,?,?,?,?,?,?,?)', 
        [method_frame.delivery_tag, str(header_frame.headers), header_frame.delivery_mode,body,
        method_frame.routing_key, method_frame.exchange,queue['name'], queue['vhost']])
    conn.commit()
    

def drain_messages(consume_channel, q, conn):
    method_frame, header_frame, body = consume_channel.basic_get(q['name'])
    while method_frame:
        write_message_to_file(conn, q, method_frame, header_frame, body)
        method_frame, header_frame, body = consume_channel.basic_get(q['name'])

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


def create_sql_tables(conn):
    print_time("Opened database successfully");
    conn.execute('''CREATE TABLE dump
       (DELIVERYTAG     INT PRIMARY KEY     NOT NULL,
       HEADER           TEXT    NULL,
       DELIVERY_MODE    INT NULL,
       BODY             BLOB     NULL,
       ROUTING_KEY      TEXT NULL,
       EXCHANGE         TEXT NULL, 
       QUEUE            TEXT,
       VHOST            TEXT NOT NULL);''')
    print_time("Table created successfully");


def dump_messages(host, vhost, user, password):
    virtual_hosts = call_api(host, vhost, user, password, "vhosts")
    for virtual_host in virtual_hosts:
        print virtual_host['name']
        
    queues = call_api(host, vhost, user, password, "queues")
    for queue in queues:
        print queue['name'] + " - " + queue['vhost']
 
    dump_dir = "dump_time_" #+ time_tostring()
    if not os.path.exists(dump_dir):
        os.makedirs(dump_dir)

    for queue in queues:
        print_time(queue['name'] + " - " + queue['vhost'])
        credentials = pika.PlainCredentials(user, password)
        connection = pika.BlockingConnection(
                pika.ConnectionParameters(host, 5672, queue['vhost'], credentials))
        channel = connection.channel()
        print_time("Dumping queue:" + queue['vhost'] + " - " + queue['name'])
        #file = open(dump_dir + "/" + queue['name'], 'w+')
        conn = sqlite3.connect(dump_dir + '/dump_' + queue['name']+ '_vhost_' +urllib.quote(queue['vhost'],safe='') +'.db')
        create_sql_tables(conn)
        drain_messages(channel, queue, conn)
        print_time("Done with queue:" + queue['name'])
        conn.close()
  
    call_api(host, vhost, user, password, "connections")

    raw_input("key to stop")

    def kill():
        channel.stop_consuming()

    connection.add_timeout(0, kill)
    print "Goodbye!"


def publish_row(row):
    delivery_mode = row[2]
    body = row[3]
    routing_key = row[4]
    exchange =  row[5]
    queue =  str(row[6])
    vhost =  row[7]
    credentials = pika.PlainCredentials(user, password)
    connection = pika.BlockingConnection(
                pika.ConnectionParameters(host, 5672, vhost, credentials))
    channel = connection.channel()
    channel.basic_publish(exchange="",
                      routing_key=queue,
                      body=body,
                      properties=pika.BasicProperties(
                         delivery_mode = delivery_mode, 

                      ))
    channel.close()
    connection.close()


def restore_messages(host, vhost, user, password):
    from multiprocessing import Pool
    from os import listdir
    from os.path import isfile, join
    dump_dir = "dump_time_"
    onlyfiles = [f for f in listdir(dump_dir) if isfile(join(dump_dir, f))]
    for file in onlyfiles:
        print_time("restoring file... " + file)
        conn = sqlite3.connect(dump_dir + '/' + file)
        cursor = conn.execute("SELECT *  from dump")
        for row in cursor:
            publish_row(row)
            #res = p.apply_async(publish_row, (row,))
            #try:
            #    res.get(timeout=20)
            #except:
            #    print "We lacked patience and got a multiprocessing.TimeoutError"

        
        conn.close()
        print_time("Restored file... " + file)




if __name__ == '__main__':
    print 'Argument list:', str(sys.argv)
    host = sys.argv[1]
    vhost = sys.argv[2]
    user = sys.argv[3]
    password = sys.argv[4]
    operation =  sys.argv[5]
    if operation == 'dump':
        print_time("****** OPERATION ******** DUMP MESSAGES ********** ")
        dump_messages(host, vhost, user, password)

    if operation == 'restore':
        restore_messages(host, vhost, user, password)
        