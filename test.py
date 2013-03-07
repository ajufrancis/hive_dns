#!/usr/bin/env python

import subprocess


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
            time_cmd = """hadoop fs -ls %s/%s/ |grep \
-w hive |awk '{print $8}'|awk -F/ '{print $7}'""" % (dirname, d)

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


if __name__ == '__main__':
    a = gen_datetime_list(dirname="/user/hive/warehouse/publicdns")
    print a
