#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function

import rospy
import mongodb_store.util as mg_util
from tqdm import tqdm
import sys
import time
import pymongo
import ssl
from multiprocessing import Process, Queue
import calendar
import datetime
import threading
import rosbag
import multiprocessing
import signal
from optparse import OptionParser
import platform
import os
from copy import copy

if float(platform.python_version()[0:2]) >= 3.0:
    import queue as qQueue
else:
    import Queue as qQueue

MongoClient = mg_util.import_MongoClient()

TIME_KEY = '_meta.inserted_at'


def max_time(collection):
    return collection.find_one(sort=[(TIME_KEY, pymongo.DESCENDING)])['_meta']['inserted_at']


def min_time(collection):
    return collection.find_one(sort=[(TIME_KEY, pymongo.ASCENDING)])['_meta']['inserted_at']


def to_ros_time(dt):
    return rospy.Time(calendar.timegm(dt.utctimetuple()), dt.microsecond * 1000)


def to_datetime(rt):
    return datetime.datetime.utcfromtimestamp(rt.secs) + datetime.timedelta(microseconds=rt.nsecs / 1000)


def ros_time_strftime(rt, format):
    """ converts a ros time to a datetime and calls strftime on it with the given format """
    return to_datetime(rt).strftime(format)


def mkdatetime(date_string):
    return datetime.datetime.strptime(date_string, '%d/%m/%y %H:%M')


class IOProcess(object):

    def __init__(self):
        super(IOProcess, self).__init__()

        self.running = multiprocessing.Value('b', True)
        self.process = multiprocessing.Process(target=self.run, args=[self.running])

    def start(self):
        self.process.start()

    def stop(self):
        self.running.value = False

    def join(self):
        self.process.join()

    def is_running(self):
        return self.running.value


class TopicReader(IOProcess):
    """ """

    def __init__(self, mongodb_host, mongodb_port, mongodb_username, mongodb_password, mongodb_authsource,
                 mongodb_certfile,
                 mongodb_ca_certs, mongodb_authmech, db_name, collection_names, start_time, end_time, queue, keep_throttled):
        super(TopicReader, self).__init__()

        self.start_time = start_time
        self.end_time = end_time

        self.mongodb_host = mongodb_host
        self.mongodb_port = mongodb_port
        self.mongodb_username = mongodb_username
        self.mongodb_password = mongodb_password
        self.mongodb_authsource = mongodb_authsource
        self.mongodb_certfile = mongodb_certfile
        self.mongodb_ca_certs = mongodb_ca_certs
        self.mongodb_authmech = mongodb_authmech
        self.db_name = db_name
        self.collection_names = collection_names
        self.queue = queue
        self.keep_throttled = keep_throttled

    def init(self, running):
        """ Called in subprocess to do process-specific initialisation """

        self.mongo_client = mongo_client(self.mongodb_host, self.mongodb_port, self.mongodb_username,
                                         self.mongodb_authsource, self.mongodb_certfile, self.mongodb_ca_certs,
                                         self.mongodb_authmech)

        if all((self.mongodb_username, self.mongodb_password)) is not None:
            self.mongo_client[self.mongodb_authsource].authenticate(self.mongodb_username, self.mongodb_password)

        # how many to
        buffer_size = 50
        # self.queue_thread = threading.Thread(target=self.queue_from_db, args=[running])

    def queue_from_db(self, running):

        for collection_name in self.collection_names:
            collection = self.mongo_client[self.db_name][collection_name]
            # make sure there's an index on time in the collection so the sort operation doesn't require the whole collection to be loaded
            collection.ensure_index(TIME_KEY)
            # get all documents within the time window, sorted ascending order by time
            documents = collection.find(
                {TIME_KEY: {'$gte': to_datetime(self.start_time), '$lte': to_datetime(self.end_time)}},
                sort=[(TIME_KEY, pymongo.ASCENDING)])

            if documents.count() == 0:
                rospy.logwarn('No messages to read from topic %s' % collection_name)
                continue
            else:
                topic = documents[0]["_meta"]["topic"]
                rospy.loginfo('Downloading %d messages for topic %s', documents.count(), topic)

            if self.keep_throttled is False:
                str_t = '_throttle'
                if topic.endswith(str_t):
                    topic = topic[:-len(str_t)]



            # load message class for this collection, they should all be the same
            msg_cls = mg_util.load_class(documents[0]["_meta"]["stored_class"])

            latch = False
            if "latch" in documents[0]["_meta"]:
                latch = documents[0]["_meta"]["latch"]

            with tqdm(total=documents.count()) as pbar:
                for document in documents:
                    pbar.update(1)
                    if running.value:
                        # instantiate the ROS message object from the dictionary retrieved from the db
                        message = mg_util.dictionary_to_message(document, msg_cls)
                        # print (message, document["_meta"]["inserted_at"])
                        # put will only work while there is space in the queue, if not it will block until another take is performed

                        try:
                            publish_time = to_ros_time(document["_meta"]["inserted_at"])
                            # rospy.loginfo('Writing msg from topic %s at time %s', topic, publish_time)
                            # bag.write(topic, message, publish_time)
                            self.queue.put((topic, message, publish_time))
                        except ValueError:
                            rospy.logerr("ValueError")
                        # self.to_publish.put((message, to_ros_time(document["_meta"]["inserted_at"])))
                    else:
                        break

            rospy.loginfo('All messages downloaded and queued for topic %s' % collection_name)
        rospy.loginfo('Finished download')
        self.queue.put('DONE')

    def run(self, running):

        self.init(running)
        self.queue_from_db(running)
        self.mongo_client.close()


class TopicWriter(IOProcess):
    def __init__(self, queue, bag_name, bag_prefix, start_time):
        super(TopicWriter, self).__init__()
        self.queue = queue
        time_str = ros_time_strftime(start_time, '%Y-%m-%d-%H-%M-%S')
        if bag_name:
            self.bag_name = bag_name + '.bag'
        elif bag_prefix:
            self.bag_name = bag_prefix + '_' + time_str + '.bag'
        else:
            self.bag_name = 'mongodb_' + time_str + '.bag'

    def queue_to_bag(self, running):

        bag = rosbag.Bag(self.bag_name, 'w')
        timeout = 1
        while running.value:
            try:
                topic_msg_time_tuple = self.queue.get(timeout=timeout)
                if topic_msg_time_tuple == 'DONE':
                    break
                topic = topic_msg_time_tuple[0]
                msg = topic_msg_time_tuple[1]
                publish_time = topic_msg_time_tuple[2]
                bag.write(topic, msg, publish_time)

            except qQueue.Empty as e:
                pass
            except ValueError:
                rospy.logerr("ValueError")
        rospy.loginfo('All messages written to %s', self.bag_name)
        # rospy.loginfo('Bag size: %s', self._sizeof_fmt(bag.size))

        bag.close()
        os.system('rosbag info ' + self.bag_name)

    def run(self, running):
        self.queue_to_bag(running)

    def _sizeof_fmt(self, num, suffix='B'):
        for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti']:
            if abs(num) < 1024.0:
                return "%3.1f%s%s" % (num, unit, suffix)
            num /= 1024.0
        return "%.1f%s%s" % (num, 'Yi', suffix)


class MongoToBag(object):
    """ Plays back stored topics from the mongodb_store """

    def __init__(self):
        super(MongoToBag, self).__init__()

        self.mongodb_host = rospy.get_param("mongodb_host")
        self.mongodb_port = rospy.get_param("mongodb_port")
        self.mongodb_username = rospy.get_param("mongodb_username", None)
        self.mongodb_password = rospy.get_param("mongodb_password", None)
        self.mongodb_authsource = rospy.get_param("mongodb_authsource", "admin")
        self.mongodb_certfile = rospy.get_param("mongodb_certfile", None)
        self.mongodb_ca_certs = rospy.get_param("mongodb_ca_certs", None)
        self.mongodb_authmech = rospy.get_param("mongodb_auth_mechanism", 'SCRAM-SHA-1')
        self.mongo_client = mongo_client(self.mongodb_host, self.mongodb_port, self.mongodb_username,
                                         self.mongodb_authsource, self.mongodb_certfile, self.mongodb_ca_certs,
                                         self.mongodb_authmech)
        self.stop_called = False
        self.queue = Queue()

    def setup(self, database_name, req_topics, start_dt, end_dt, bag_prefix, bag_name, keep_throttled):
        """ Read in details of requested playback collections. """

        if self.mongodb_password is not None:
            self.mongo_client[self.mongodb_authsource].authenticate(self.mongodb_username, self.mongodb_password)

        if database_name not in self.mongo_client.database_names():
            raise Exception('Unknown database %s' % database_name)

        database = self.mongo_client[database_name]
        collection_names = database.collection_names(include_system_collections=False)

        req_topics = set(map(mg_util.topic_name_to_collection_name, req_topics))

        if len(req_topics) > 0:
            topics = req_topics.intersection(collection_names)
            dropped = req_topics.difference(topics)
            if (len(dropped) > 0):
                print('WARNING Dropped non-existant requested topics for playback: %s' % dropped)
        else:
            topics = set(collection_names)


        print('Reading topics %s' % topics)

        if keep_throttled is False:
            rospy.loginfo("Removing _throttle suffix from topics")

        # create mongo collections
        collections = [database[collection_name] for collection_name in topics]

        # make sure they're easily accessible by time
        for collection in collections:
            collection.ensure_index(TIME_KEY)

        if len(start_dt) == 0:
            # get the min and max time across all collections, conver to ros time
            start_time = to_ros_time(
                min(map(min_time, [collection for collection in collections if collection.count() > 0])))
        else:
            start_time = to_ros_time(mkdatetime(start_dt))

        if len(end_dt) == 0:
            end_time = to_ros_time(
                max(map(max_time, [collection for collection in collections if collection.count() > 0])))
        else:
            end_time = to_ros_time(mkdatetime(end_dt))

        # we don't need a connection any more
        self.mongo_client.close()

        # rospy.loginfo('Playing back from %s' % to_datetime(start_time))
        # rospy.loginfo('.............. to %s' % to_datetime(end_time))

        pre_roll = rospy.Duration(2)
        post_roll = rospy.Duration(0)

        # create playback objects
        self.reader = TopicReader(self.mongodb_host, self.mongodb_port, self.mongodb_username,
                                  self.mongodb_password, self.mongodb_authsource, self.mongodb_certfile,
                                  self.mongodb_ca_certs, self.mongodb_authmech, database_name, topics,
                                  start_time - pre_roll, end_time + post_roll, self.queue, keep_throttled)
        self.writer = TopicWriter(self.queue, bag_name, bag_prefix, start_time)

    def start(self):
        self.reader.start()
        self.writer.start()

    def join(self):
        self.reader.join()
        self.writer.join()

    def stop(self):
        self.stop_called = True
        self.reader.stop()
        self.writer.stop()

    def is_running(self):
        return True


def mongo_client(host, port, username=None, authsource="admin", certfile=None, ca_certs=None, authmech='SCRAM-SHA-1'):
    if username is not None:
        rospy.logdebug("Connecting with authentication")
        use_ssl = all((certfile, ca_certs))
        if use_ssl:
            rospy.logdebug("Connecting with SSL encryption")
        client = MongoClient(host, port,
                             ssl=use_ssl,
                             ssl_certfile=certfile,
                             ssl_cert_reqs=ssl.CERT_REQUIRED,
                             ssl_ca_certs=ca_certs,
                             authMechanism=authmech)
    else:
        client = MongoClient(host, port)
    return client


def main(argv):
    myargv = rospy.myargv(argv=argv)

    parser = OptionParser()
    parser.usage += " [TOPICs...]"
    parser.add_option("--mongodb-name", dest="mongodb_name",
                      help="Name of DB from which to retrieve values",
                      metavar="NAME", default="roslog")
    parser.add_option("-s", "--start", dest="start", type="string", default="", metavar='S',
                      help='start datetime of query, defaults to the earliest date stored in db, across all requested collections. Formatted "d/m/y H:M" e.g. "06/07/14 06:38"')
    parser.add_option("-e", "--end", dest="end", type="string", default="", metavar='E',
                      help='end datetime of query, defaults to the latest date stored in db, across all requested collections. Formatted "d/m/y H:M" e.g. "06/07/14 06:38"')
    parser.add_option("-o", "--output-prefix", dest="bag_prefix", type="string", default="", metavar='PREFIX',
                      help='prepend PREFIX to beginning of bag name (name will always end with date stamp of query start)')
    parser.add_option("-O", "--output-name", dest="bag_name", type="string", default="", metavar='NAME',
                      help='record to bag with name NAME.bag')
    parser.add_option("-t", "--keep-throttled", dest="keep_throttled", action='store_true',
                      help='If true, _throttle suffix will not be removed from topics')
    parser.set_defaults(keep_throttled=False)

    (options, args) = parser.parse_args(rospy.myargv(argv=sys.argv)[1:])

    database_name = options.mongodb_name

    if args[0] == 'PARAM':
        print("Reading topics from parameter server")
        topics = rospy.get_param("log_topics")
    else:
        topics = args

    rospy.init_node("mongodb_to_rosbag", log_level=rospy.INFO)
    mongo_to_bag = MongoToBag()

    def signal_handler(signal, frame):
        mongo_to_bag.stop()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    mongo_to_bag.setup(database_name, topics, options.start, options.end, options.bag_prefix, options.bag_name,
                       options.keep_throttled)
    mongo_to_bag.start()
    mongo_to_bag.join()


# processes load main so move init_node out
if __name__ == "__main__":
    main(sys.argv)
