import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl

import argparse
import subprocess
import datetime
import signal
import sys
import time

# Set the default color cycle
#mpl.rcParams['axes.prop_cycle'] = mpl.cycler(color=["r", "k", "b"])
swap_size= 4 * 1024 # MB

util_d = {'system': [], 'mem': [], 'time': []}

def plot_utilization(output_f):
    util_df = pd.DataFrame.from_dict(util_d)
    util_df.set_index('time', inplace=True)

    figure, axes = plt.subplots(nrows=1, ncols=1, sharex=True)
    #util_df['cpu'].plot(legend=True, ax=axes[0])
    util_df.groupby('system')['mem'].plot(legend=True, ax=axes)
    #axes[0].set_ylabel("CPU usage (%)")
    #axes[0].set_ylim(0, 102)
    axes.set_ylabel("Memory usage (GB)")
    axes.set_ylim(0, 6.0)
    axes.set_xlabel("Time (sec)")
    plt.savefig(output_f)

def main(interval, total_time, output):

    start_ts = datetime.datetime.now()
    for _ in range(int(total_time)):
        result_mem = subprocess.run(['bash', '-c',
            "free -m | grep Mem | awk '{print $3}'"],
            stdout=subprocess.PIPE)

        result_cpu = subprocess.run(['bash', '-c',
            "grep 'cpu ' /proc/stat | awk '{usage=($2+$4)*100/($2+$4+$5)} " +
            "END {print usage}'"], stdout=subprocess.PIPE)

        curr_ts = (datetime.datetime.now() - start_ts).total_seconds()
        mem_output = result_mem.stdout.decode('utf-8').replace('%','')
        cpu_output = result_cpu.stdout.decode('utf-8').replace('%','')
        try:
            line_mem = mem_output.split('\n')[0]
            line_cpu = cpu_output.split('\n')[0]
            util_d['system'].append('mcswap')
            #util_d['cpu'].append(float(line_cpu))
            util_d['mem'].append(int(line_mem) / 1024)
            util_d['time'].append(curr_ts)

            util_d['system'].append('fastswap')
            util_d['mem'].append(swap_size / 1024)
            util_d['time'].append(curr_ts)
        except:
            print('Error while parsing metrics from metrics-server')

        time.sleep(interval)


    print(util_d)
    plot_utilization(output)

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--interval', help='interval (sec) to monitor CPU/MEM',
                        required=False, default=1)
    parser.add_argument('--count', help='number of intervals to monitor CPU/MEM',
                        required=False, default=60)
    parser.add_argument('--output', help='file to store plot', required=True)
    args = parser.parse_args()
    main(args.interval, args.count, args.output)

