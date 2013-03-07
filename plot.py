#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import subprocess
from pylab import *
import multiprocessing
import matplotlib.pyplot as plt


def most_plot(pdict={}, plot_title='default', output_name='default.png'):
    """
        Plot the Dict as barh chart, key is the ip address, value is the times
    """
    ka = []
    va = []

    # sort by value (request times)
    for key, value in sorted(pdict.iteritems(), key=lambda(k, v): (int(v), k)):
        ka.append(str(key))
        va.append(int(value))

    val = va
    #pos = arange(len(ka)) + .4    # the bar centers on the y axis
    pos = arange(len(ka))
    fig = plt.figure(1, figsize=(18, 6), frameon=False)
    fig.subplots_adjust(left=0.23)
    barh(pos, val, align='center', height=0.5)
    yticks(pos, ka)
    xlabel('Request Times')
    title(plot_title)
    grid(True)
    fig.savefig(output_name)
    close()


def top_dns_col_plot(number, start, end):
    function = 'top_col'
    col_list = ['clientip', 'queryentry', 'class', 'type', 'recursion',
                'dnsserver']
    date = ''.join(start.split('-')[:-1])
    for col in col_list:
        query_cmd = """./hive.py -f %s -c %s -s %s -e %s -n %s """ %  \
            (function, col, start, end, number)
        print query_cmd
        mkdir_cmd = "mkdir -p /var/www/static/%s" % date
        # Make directory by date
        subprocess.Popen(mkdir_cmd, shell=True, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
        # start query
        child = subprocess.Popen(query_cmd, shell=True, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
        (stdout, stderr) = child.communicate()
        if child.returncode == 0:
            print stdout
            a = json.loads(stdout)
            title = "%s \"%s\" %s To %s Top %s Entries" % (
                function, col, start, end, number)
            fname = "%s_%s_%s_%s_%s" % (function, col, start, end, number)
            output_name = "/var/www/static/%s/%s.png" % (date, fname)
            most_plot(pdict=a, plot_title=title, output_name=output_name)
        else:
            print "Error Running cmd: %s, %s" % (query_cmd, stderr)


def gen_datetime_list(dirname):
    """
    List the HDFS file system, get the datetime name which like 2013-02-15-03
    """
    dirname = dirname
    datetime_list = []
    date_cmd = "hadoop fs -ls %s|grep hive |awk '{print $8}'|awk -F/ '{print $6}'" % dirname
    date_child = subprocess.Popen(
        date_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (stdout, stderr) = date_child.communicate()
    if date_child.returncode == 0:
        date = [i for i in stdout.split()]
        for d in date:
            # 2013-02-15 like this
            d_dash = d[:4] + '-' + d[4:6] + '-' + d[6:]
            time_cmd = """hadoop fs -ls %s/%s/ |grep -w hive |awk '{print $8}'|awk -F/ '{print $7}'""" % (dirname, d)

            time_child = subprocess.Popen(
                time_cmd, shell=True, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
            (stdout, stderr) = time_child.communicate()
            if time_child.returncode == 0:
                time = [i for i in stdout.split()]
                for t in time:
                    # 2013-02-15-03 like this
                    d_t_dash = d_dash + '-' + t
                    datetime_list.append(d_t_dash)

    else:
        print "Error Running cmd: %s, %s" % (date_cmd, stderr)

    return datetime_list

def main():
    time_spot = gen_datetime_list(dirname="/user/hive/warehouse/publicdns")

    # Multi Process to run top_dns_col_plot function
    # top_dns_col_plot(number=number, start=start1, end=start1)
    pool = multiprocessing.Pool(processes=10)
    for i in time_spot:
        pool.apply_async(top_dns_col_plot, (15, i, i))
    pool.close()
    pool.join()
    print "Sub-process(es) done."

if __name__ == '__main__':
    main()
