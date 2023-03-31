#!/bin/python3

import os
import argparse
import time

parser = argparse.ArgumentParser(
    prog='sysbench-test',
    description='Do the overlap and trans test',
    epilog='Text at the bottom of help')
parser.add_argument("--p", action="store_true",
                    default=False, help="Only pepare.")
parser.add_argument("--r", action="store_true",
                    default=False, help="Only run.")
parser.add_argument("--b", action="store_true",
                    default=False, help="Only burst test.")
parser.add_argument("--pr", action="store_true",
                    default=False, help="Prepare and run (clean up).")
parser.add_argument("--pb", action="store_true",
                    default=False, help="Prepare and burst test.")
parser.add_argument("--z", action="store_true",
                    default=False, help="If use zipfian random")
parser.add_argument("--h", action="store_true",
                    default=False, help="If use zipfian random")
parser.add_argument("--ph", action="store_true",
                    default=False, help="If use zipfian random")

build_dir = "/flash1/chuannan/alisql_build"
data_dir = "/flash1/chuannan/data"
tables = 16
bench_name = "oltp_update_non_index"
mysql_host = ["us-instance-1.cehzqyob85vz.us-west-1.rds.amazonaws.com","ap-cluster.cluster-ro-cezvim4cz4mz.ap-southeast-1.rds.amazonaws.com","eu-instance.chtkxg6zlzeb.eu-central-1.rds.amazonaws.com"]
mysql_port = [3306,3306,3306]
mysql_user = "bc"
mysql_password = "bc123456"
mysql_db = "test"
mysql_ignore_errors = "1213,1020,1205,3101"
run_time = 60
warmup_time = run_time//2
MAX_RANGE_UPDATE = 100000


node_num = 3
node_id = 1 #
auto_inc = "off"
prepare_range = [1,2000000]

overlap = 0 #
node_run_range = [
    [1,600000],
    [600001,1200000],
    [1200001,1800000]
]
# overlap = 10 
# node_run_range = [
#     [1,540000,1620001,1680000],
#     [540001,1080000,1620001,1680000],
#     [1080001,1680000]
# ]
# overlap = 20
# node_run_range = [
#     [1,480000,1440001,1560000],
#     [480001,960000,1440001,1560000],
#     [960001,1560000]
# ]
# overlap = 50
# node_run_range = [
#     [1,300000,900001,1200000],
#     [300001,600000,900001,1200000],
#     [600001,1200000]
# ]
table_size = 2000000
thd_list = [50,100,200,400,600]


def sysbench_cmd_formatter(host, port, user, password, db, bench_name, cmd, run_time, warmup_time, thd=1, range_list=[]):
    # check param
    if (type(host) != str or type(port) != int or type(user) != str or type(db) != str):
        print("Check again")
        return
    print("Sysbench cmd formatter with:")
    print("mysql host:{}\tport:{}\tuser:{}".format(host, port, user, db))
    mysql_conf = "sysbench --db-driver=mysql --mysql-host={} --mysql-port={} --mysql-user={} --mysql-password={} --mysql-db={} --mysql-ignore-errors={} ".format(
        host, port, user, password, db, mysql_ignore_errors)
    if args.z:
        rand_type = "zipfian"
    else:
        rand_type = "uniform"
    if (cmd == "prepare"):
        if (range_list != []):
            prepare_list = ",".join(str(i) for i in range_list)
            bench_conf = "--report-interval=1  --auto_inc={} --tables={} --prepare_range={} --rand-type={} --threads={} --create_secondary=false {} {}".format(
                auto_inc, tables, prepare_list, rand_type, thd, bench_name, cmd)
        else:
            bench_conf = "--report-interval=1  --auto_inc={} --tables={} --table_size={} --rand-type={} --threads={} --create_secondary=false {} {}".format(
                auto_inc, tables, table_size, rand_type, thd, bench_name, cmd)
    elif (cmd == "run"):
        if (range_list != []):
            run_list = ",".join(str(i) for i in range_list)
            bench_conf = "--report-interval=1  --auto_inc={} --tables={} --run_range={}  --rand-type={} --time={} --warmup-time={} --threads={} --create_secondary=false {} {}".format(
                auto_inc, tables, run_list, rand_type, run_time, warmup_time, thd, bench_name, cmd)
        else:
            bench_conf = "--report-interval=1  --auto_inc={} --tables={} --table_size={}  --rand-type={} --time={} --warmup-time={} --threads={} --create_secondary=false {} {}".format(
                auto_inc, tables, table_size, rand_type, run_time, warmup_time, thd, bench_name, cmd)
    elif (cmd == "cleanup"):
        bench_conf = "--report-interval=1  --auto_inc={} --tables={} --table_size={} --time={} --threads={} --create_secondary=false {} {}".format(
            auto_inc, tables, table_size, run_time, thd, bench_name, cmd)
    print(mysql_conf + bench_conf)
    return mysql_conf + bench_conf


def sysbench_prepare(prepare_list):
    # node 2 prepare
    # if skip?
    print("Node 1 prepare the test set first.")
    return os.system(sysbench_cmd_formatter(mysql_host[0], mysql_port[0], mysql_user, mysql_password, mysql_db, bench_name, "prepare", run_time, warmup_time, 2, prepare_list))

def sysbench_run_test(run_list):
    node1_run_cmd = "taskset -c 0-7 " + sysbench_cmd_formatter(
        mysql_host[0], mysql_port[0], mysql_user, mysql_password, mysql_db, bench_name, "run", run_time, warmup_time, 8, run_list)
    err1 = os.system(
        node1_run_cmd+"  2>&1 > test_sysbench.txt &")

def sysbench_run_range(no,run_list,overlap=0, thd=0):
    assert(thd != 0)
    print(f"Sysench run on Node{no+1}! Specific run_list={run_list}")
    cmd = sysbench_cmd_formatter(
        mysql_host[no],mysql_port[no],mysql_user, mysql_password, mysql_db, bench_name, "run", run_time, warmup_time, thd, run_list)
    err = os.system(
        cmd+"  2>&1 > overlap{}_thd{}_node{}.txt &".format(overlap, thd, no+1))
    if(err):
        print("Run error, please check result")

def sysbench_run(overlap=0, thd=0):
    # calculate the range
    print("Attempt to run with overlap:{}, thd:{}".format(overlap, thd))
    half_size = table_size//2
    # mix_range = half_size*overlap//(200-overlap)
    mix_range = half_size*overlap//200
    node1_range = [1+mix_range, half_size+mix_range]
    # node2_range = [half_size-mix_range+1, table_size-mix_range]
    print(f"node2:{node1_range}")
    # prepare cmd
    assert (thd != 0)
    # if need taskset?
    node1_run_cmd = sysbench_cmd_formatter(
        mysql_host[0], mysql_port[0], mysql_user, mysql_password, mysql_db, bench_name, "run", run_time, warmup_time, thd, node1_range)
    # node2_run_cmd = sysbench_cmd_formatter(
    #     mysql_host[1], mysql_port[1], mysql_user, mysql_password, mysql_db, bench_name, "run", run_time, warmup_time, thd, node2_range)
    # node1 run
    # print(node1_run_cmd+"  2>&1 >> overlap{}_thd{}_node1".format(overlap,thd))
    err1 = os.system(
        node1_run_cmd+"  2>&1 > overlap{}_thd{}_node1.txt &".format(overlap, thd))
    # err2 = os.system(
    #     node2_run_cmd+"  2>&1 > overlap{}_thd{}_node2.txt &".format(overlap, thd))
    # wait a while
    time.sleep(run_time + warmup_time + 5)
    if (err1):
        print("Run error, please check result")


def sysbench_cleanup():
    # cleanup
    # node 2 cleanup
    return os.system(sysbench_cmd_formatter(mysql_host[0], mysql_port[0], mysql_user, mysql_password, mysql_db, bench_name, "cleanup", run_time, warmup_time))


def sysbench_burst(before=50, after=60, thd=0):
    node1_before_range = [prepare_range[0],
                          prepare_range[0]+table_size*before//100-1]
    node2_before_range = [prepare_range[0] +
                          table_size*before//100, prepare_range[1]]
    node1_after_range = [prepare_range[0],
                         prepare_range[0]+table_size*after//100-1]
    node2_after_range = [prepare_range[0] +
                         table_size*after//100, prepare_range[1]]
    print(f"node1_before:{node1_before_range}",
          f"node2_before:{node2_before_range}")
    node1_run_cmd = "taskset -c 0-7 " + sysbench_cmd_formatter(
        mysql_host[0], mysql_port[0], mysql_user, mysql_password, mysql_db, bench_name, "run", run_time, warmup_time, thd, node1_before_range)
    node2_run_cmd = "taskset -c 8-15 " + sysbench_cmd_formatter(
        mysql_host[0], mysql_port[1], mysql_user, mysql_password, mysql_db, bench_name, "run", run_time, warmup_time, thd, node2_before_range)
    err1 = os.system(
        node1_run_cmd+"  2>&1 > before{}_thd{}_node1.txt &".format(before, thd))
    err2 = os.system(
        node2_run_cmd+"  2>&1 > before{}_thd{}_node2.txt &".format(before, thd))
    time.sleep(run_time + warmup_time+5)
    print(f"node1_after:{node1_after_range}",
          f"node2_after:{node2_after_range}")
    node1_run_cmd = "taskset -c 0-7 " + sysbench_cmd_formatter(
        mysql_host[0], mysql_port[0], mysql_user, mysql_password, mysql_db, bench_name, "run", run_time*2, 0, thd, node1_after_range)
    node2_run_cmd = "taskset -c 8-15 " + sysbench_cmd_formatter(
        mysql_host[0], mysql_port[1], mysql_user, mysql_password, mysql_db, bench_name, "run", run_time*2, 0, thd, node2_after_range)
    err1 = os.system(
        node1_run_cmd+"  2>&1 > after{}_thd{}_node1.txt &".format(after, thd))
    err2 = os.system(
        node2_run_cmd+"  2>&1 > after{}_thd{}_node2.txt &".format(after, thd))
    time.sleep(run_time + warmup_time+5)


def sysbench_burst_target(before=50, after=60, thd=0):
    node1_before_range = [prepare_range[0],
                          prepare_range[0]+table_size*before//100-1]
    node2_before_range = [prepare_range[0] +
                          table_size*before//100, prepare_range[1]]
    node1_after_range = [prepare_range[0]+table_size*before//100,
                         prepare_range[0]+table_size*after//100-1]
    node2_after_range = [prepare_range[0] +
                         table_size*after//100, prepare_range[1]]
    print(f"node1_before:{node1_before_range}",
          f"node2_before:{node2_before_range}")
    node1_run_cmd = "taskset -c 0-7 " + sysbench_cmd_formatter(
        mysql_host[0], mysql_port[0], mysql_user, mysql_password, mysql_db, bench_name, "run", run_time, warmup_time, thd, node1_before_range)
    node2_run_cmd = "taskset -c 8-15 " + sysbench_cmd_formatter(
        mysql_host[0], mysql_port[1], mysql_user, mysql_password, mysql_db, bench_name, "run", run_time, warmup_time, thd, node2_before_range)
    err1 = os.system(
        node1_run_cmd+"  2>&1 > before{}_thd{}_node1.txt &".format(before, thd))
    err2 = os.system(
        node2_run_cmd+"  2>&1 > before{}_thd{}_node2.txt &".format(before, thd))
    time.sleep(run_time + warmup_time+5)
    print(f"node1_after:{node1_after_range}",
          f"node2_after:{node2_after_range}")
    node1_run_cmd = "taskset -c 0-7 " + sysbench_cmd_formatter(
        mysql_host[0], mysql_port[0], mysql_user, mysql_password, mysql_db, bench_name, "run", run_time*2, 0, thd, node1_after_range)
    node2_run_cmd = "taskset -c 8-15 " + sysbench_cmd_formatter(
        mysql_host[0], mysql_port[1], mysql_user, mysql_password, mysql_db, bench_name, "run", run_time*2, 0, thd, node2_after_range)
    err1 = os.system(
        node1_run_cmd+"  2>&1 > after{}_thd{}_node1.txt &".format(after, thd))
    err2 = os.system(
        node2_run_cmd+"  2>&1 > after{}_thd{}_node2.txt &".format(after, thd))
    time.sleep(run_time + warmup_time+5)


def sysbench_hack(tables):
    for table in range(1, tables+1):
        for i in range(1,node_num):
            # get range
            overwrite_stmt = f"UPDATE test1.sbtest{table} SET RowMeta=unhex('0{i}001D00000000000000') WHERE id>={node_run_range[i][0]} and id<={node_run_range[i][1]}"
            print(
                f"Hack table {table}, where id>={node_run_range[i][0]} and id<={node_run_range[i][1]}")
            os.system(
                f"mysql -u{mysql_user} -h 172.16.162.185 -P3751 -p{mysql_password} -e \"set global row_trans_batch=ON;\"")
            os.system(
                f"mysql -u{mysql_user} -h 172.16.162.185 -P3751 -p{mysql_password} -e \"{overwrite_stmt}\"")
            os.system(
                f"mysql -u{mysql_user} -h 172.16.162.185 -P3751 -p{mysql_password} -e \"set global row_trans_batch=OFF;\"")


if __name__ == "__main__":
    args = parser.parse_args()
    if (args.p or args.pr or args.pb or args.ph):
        sysbench_cleanup()
        sysbench_prepare(prepare_range)
    if (args.h or args.ph):
        sysbench_hack(tables)
    if (args.r or args.pr):
        # sysbench_run_test(preparbe_range)
        for thd in thd_list:
            # sysbench_run(50, thd)
            sysbench_run_range(node_id,node_run_range[node_id],overlap,thd)
            time.sleep(run_time + warmup_time + 60)
    elif (args.b or args.pb):
        # birst before
        # sysbench_burst(thd=32)
        sysbench_burst_target(thd=32)
