#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function

import rospy
import mongodb_store.util as mg_util
from tqdm import tqdm, trange
import sys
import time
import pymongo
import ssl
from multiprocessing import Process
import calendar
import datetime
import threading
import rosbag
import multiprocessing
from rosgraph_msgs.msg import Clock
import signal
from optparse import OptionParser
import platform

if float(platform.python_version()[0:2]) >= 3.0:
    import queue as Queue
else:
    import Queue

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


class PlayerProcess(object):

    def __init__(self, start_time, end_time):
        super(PlayerProcess, self).__init__()

        self.start_time = start_time
        self.end_time = end_time

        self.running = multiprocessing.Value('b', True)
        self.player_process = multiprocessing.Process(target=self.run, args=[self.running])

    def start(self):
        self.player_process.start()

    def stop(self):
        self.running.value = False

    def join(self):
        self.player_process.join()

    def is_running(self):
        return self.running.value


class TopicReader(PlayerProcess):
    """ """

    def __init__(self, mongodb_host, mongodb_port, mongodb_username, mongodb_password, mongodb_authsource, mongodb_certfile,
                 mongodb_ca_certs, mongodb_authmech, db_name, collection_names, start_time, end_time, queue=None):
        super(TopicReader, self).__init__(start_time, end_time)

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
        bag = rosbag.Bag('test.bag', 'w')
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
                return
            else:
                rospy.logdebug('Storing %d messages for topic %s', documents.count(), collection_name)

            # load message class for this collection, they should all be the same
            msg_cls = mg_util.load_class(documents[0]["_meta"]["stored_class"])

            latch = False
            if "latch" in documents[0]["_meta"]:
                latch = documents[0]["_meta"]["latch"]

            topic = documents[0]["_meta"]["topic"]

            with tqdm(total=documents.count()) as pbar:
                for document in documents:
                    pbar.update(1)
                    if running.value:
                        # instantiate the ROS message object from the dictionary retrieved from the db
                        message = mg_util.dictionary_to_message(document, msg_cls)
                        # print (message, document["_meta"]["inserted_at"])
                        # put will only work while there is space in the queue, if not it will block until another take is performed

                        try:
                            publish_time=to_ros_time(document["_meta"]["inserted_at"])
                            # rospy.loginfo('Writing msg from topic %s at time %s', topic, publish_time)
                            bag.write(topic, message, publish_time)
                        except ValueError:
                            rospy.logerr("ValueError")
                        #self.to_publish.put((message, to_ros_time(document["_meta"]["inserted_at"])))
                    else:
                        break

            rospy.logdebug('All messages stored for topic %s' % collection_name)
        bag.close()

    def run(self, running):

        self.init(running)
        self.queue_from_db(running)
        # self.queue_thread.join()
        self.mongo_client.close()



class MongoPlayback(object):
    """ Plays back stored topics from the mongodb_store """

    def __init__(self):
        super(MongoPlayback, self).__init__()

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

    def setup(self, database_name, req_topics, start_dt, end_dt):
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

        print('Playing back topics %s' % topics)

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

        self.event = multiprocessing.Event()

        # create clock thread
        pre_roll = rospy.Duration(2)
        post_roll = rospy.Duration(0)
        # self.clock_player = ClockPlayer(self.event, start_time, end_time, pre_roll, post_roll)

        # create playback objects
        self.reader = TopicReader(self.mongodb_host, self.mongodb_port, self.mongodb_username,
                                                 self.mongodb_password, self.mongodb_authsource, self.mongodb_certfile,
                                                 self.mongodb_ca_certs, self.mongodb_authmech, database_name, topics,
                                                 start_time - pre_roll, end_time + post_roll)

    def start(self):

        # this creates new processes and publishers for each topic
        self.reader.start()

        # all players wait for this before starting --
        # todo: it could happen that his gets hit before all are constructed though
        self.event.set()

    def join(self):

        # self.clock_player.join()

        # if clock runs out but we weren't killed then we need ot stop other processes
        self.reader.join()

    def stop(self):
        self.stop_called = True
        # self.clock_player.stop()
        self.reader.stop()

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
    (options, args) = parser.parse_args(myargv)

    database_name = options.mongodb_name
    topics = set(args[1:])
    rospy.init_node("mongodb_to_rosbag", log_level=rospy.DEBUG)
    playback = MongoPlayback()

    def signal_handler(signal, frame):
        playback.stop()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    playback.setup(database_name, topics, options.start, options.end)
    playback.start()
    playback.join()
    rospy.set_param('use_sim_time', False)


# processes load main so move init_node out
if __name__ == "__main__":
    main(sys.argv)
