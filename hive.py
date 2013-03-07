#!/usr/bin/env jython
# -*- coding: utf-8 -*-

import sys
import time
import random
import logging
from java.lang import *
from java.lang import *
from java.sql import *
import simplejson as json
from optparse import OptionParser
from datetime import datetime, timedelta as td

logging.basicConfig(filename="execute.log", level=logging.INFO,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


class Connection(object):
    """
    Connection to JDBC hive, create table and run sql query
    """
    def _randam_table(self):
        """
        Generate a Random Table Name
        """
        salt = random.randint(10000000, 99999999)
        now = time.time()
        table_name = "t" + str(int(now)) + str(salt)
        #print "Will create Table with Name: ", table_name
        return table_name

    def __init__(self,
                 start,
                 end,
                 hive_url=None,
                 hdfs_location=None
                 ):

        self.hive_url = hive_url
        self.hdfs_location = hdfs_location
        self.table_name = self._randam_table()
        self.start = start
        self.end = end
        self.partition_array = []
        self.conn = None
        self.stmt = None

        driverName = "org.apache.hadoop.hive.jdbc.HiveDriver"

        if not self.hdfs_location:
            self.hdfs_location = "/user/hive/warehouse/publicdns"

        if not self.hive_url:
            self.hive_url = "jdbc:hive://hive-service-host:10000/default"

        # Load Driver
        try:
            Class.forName(driverName)
        except Exception, e:
            logging.error("Unable to load Driver %s" % driverName)
            logging.error(str(e))

        # Start connect remote hive service
        self.conn = DriverManager.getConnection(self.hive_url, "", "")
        self.stmt = self.conn.createStatement()

        sql_create_table = """
        CREATE EXTERNAL TABLE IF NOT EXISTS  """ + self.table_name + """(
            clientip STRING, clientport STRING,
            queryentry STRING, class STRING, type STRING, recursion STRING,
            options STRING, dnsserver STRING)
        PARTITIONED BY(dt STRING)
        ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
        WITH SERDEPROPERTIES (
            "input.regex" = "^.*queries: client ([0-9\.]*)#([0-9]*).*query: ([^ ]*) ([^ ]*) ([^ ]*) ([+-]*)([A-Z]*)?[ (]*([0-9\.]*)?.?$",
            "output.format.string" = "%1$s %2$s %3$s %4$s %5$s %6$s %7$s %8$s"
        )
        STORED AS TEXTFILE
        LOCATION '""" + self.hdfs_location + """'"""

        # Create a table
        logging.info(sql_create_table)
        self.stmt.executeQuery(sql_create_table)

        logging.info("Table %s created" % self.table_name)

        # create partitions base on start and end date time
        self._pre_work()
        self._create_partition()

    def _gen_dt(self, start, end):
        # start and end like: 2013-02-20-02
        start_s = start.split('-')
        d1 = datetime(int(
            start_s[0]), int(start_s[1]), int(start_s[2]), int(start_s[3]))

        end_s = end.split('-')
        d2 = datetime(
            int(end_s[0]), int(end_s[1]), int(end_s[2]), int(end_s[3]))

        partition_array = []

        # caculate the days,hours between start and end
        delta = d2 - d1
        for hr in range(delta.days * 24 + delta.seconds // 3600 + 1):
            partition_array.append(d1 + td(hours=hr))

        return partition_array

    def _create_partition(self):
        # start and end like: 2013-02-20-02

        self.partition_array = self._gen_dt(self.start, self.end)
        for i in self.partition_array:
            sql_partition = """
            ALTER TABLE %s
            ADD PARTITION (dt = '%s')
            LOCATION '%s/%s'""" % (self.table_name,
                                   i.strftime("%Y-%m-%d-%H"),
                                   self.hdfs_location,
                                   i.strftime("%Y%m%d/%H"))
            logging.info(sql_partition)
            # create partition
            self.stmt.executeQuery(sql_partition)

    def _pre_work(self):
        # Add JAR for map reduce
        sql_add_jar = "add JAR /usr/lib/hive/lib/hive-contrib-0.7.1-cdh3u5.jar"
        logging.info(sql_add_jar)
        self.stmt.executeQuery(sql_add_jar)

    def show_tables(self):
        # Show tables
        res = self.stmt.executeQuery("SHOW TABLES")
        print "List of tables:"
        while res.next():
            print res.getString(1)

    def top_col(self, column, number=10, where=None):
        """
        Query the most number (10) inside the table
        """

        if not where:
            sql_most_n = """
            SELECT %s , COUNT(1) AS numrequest FROM %s
            where dt >= '%s' and dt <= '%s'
            GROUP BY %s SORT BY numrequest DESC LIMIT %s
            """ % (column, self.table_name, self.start, self.end, column, number)
        else:
            (where_k, where_v) = where.split('=')
            sql_most_n = """
            SELECT %s , COUNT(1) AS numrequest FROM %s
            where dt >= '%s' and dt <= '%s' and %s = '%s'
            GROUP BY %s SORT BY numrequest DESC LIMIT %s
            """ % (column, self.table_name, self.start, self.end,
                   where_k, where_v, column, number)

        # SELECT the data
        logging.info(
            "Calculating most %s %s and request number:" % (number, column))

        logging.info(sql_most_n)
        res = self.stmt.executeQuery(sql_most_n)

        # Retrun result
        dict_results = {}
        while res.next():
            dict_results[res.getString(1)] = res.getInt(2)

        return dict_results

    def count_entry(self, where=None):
        """
        Count the Entries number from table, between start and end
        """
        if not where:
            sql_count = """
            SELECT COUNT(*)  FROM %s
            where dt >= '%s' and dt <= '%s'
            """ % (self.table_name, self.start, self.end)
        else:
            (where_k, where_v) = where.split('=')
            sql_count = """
            SELECT COUNT(*)  FROM %s
            where dt >= '%s' and dt <= '%s' and  %s = '%s'
            """ % (self.table_name, self.start, self.end, where_k, where_v)

        # SELECT the data
        logging.info(
            "Calculating count entries btween %s and  %s " % (self.start, self.end))
        logging.info(sql_count)
        res = self.stmt.executeQuery(sql_count)
        # Retrun result
        while res.next():
            result = res.getInt(1)
        return result

    def drop_table(self):
        """
        Drop the temp table
        """
        # Drop table
        logging.info("Droping table %s" % self.table_name)
        self.stmt.executeQuery("DROP TABLE " + self.table_name)


def parseopt():
    usage = "usage: %prog -f [function name] -s [start] \
-e [end] <-c [column name]> <-n [number]> <-w [key=value]>"
    parser = OptionParser(usage=usage)
    parser.add_option('-f', '--function', action='store',
                      help='Which function you need, For DNS log we \
                      support: top_col, count_entry')
    parser.add_option('-s', '--start', action='store',
                      help='Indicate the Start DateTime, such as \
                      2013-02-20-01, which means 2013/02/20 1 clock')
    parser.add_option('-e', '--end', action='store',
                      help='Indicate the End DateTime, such as \
                      2013-02-21-14, which means 2013/02/21 14 clock')
    parser.add_option('-c', '--column', action='store',
                      help="""Which column need calculate, it needed for \
                      top_col function.  For DNS log we \
                      support:  clientip, queryentry, class, type, \
                      recursion, options, dnsserver""")
    parser.add_option('-n', '--number', action='store', type="int",
                      help='Optional, Limit the output entries for the \
                      query from Hive. Default is 10.')
    parser.add_option('-w', '--where', action='store',
                      help='Optional, Limit the query condition, \
                      such as clientip=\'10.10.1.1\' ')
    (options, args) = parser.parse_args(sys.argv)
    # all options are must except number which default is 10
    if not (options.function and options.start and options.end):
        parser.print_help()
        sys.exit(1)
    return options, args


def main():
    options, args = parseopt()
    a = Connection(start=options.start, end=options.end)
    if options.function == 'top_col':
        # set default limit number value
        num = 10
        if options.number is not  None:
            num = options.number
        # do the calcualtion
        results = a.top_col(column=options.column, number=num,
                            where=options.where)
        print json.dumps(results)
    elif options.function == 'count_entry':
        # do the calcualtion
        r = {}
        number = a.count_entry(where=options.where)
        k = options.start + "_" + options.end
        r[k] = number
        print json.dumps(r)

    # don't forget drop this temp table
    a.drop_table()
    sys.exit(0)

if __name__ == '__main__':
    main()
