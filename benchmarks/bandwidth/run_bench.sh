#!/bin/bash

SWAP_PARTITION=/dev/nvme0n1p3
CLIENT_IB_IF='ib0'

SERVER_IP='192.168.1.20'
SERVER_PORT=10000

RESULTS_DIR=./results

CLIENT_IP=`ip -o -4 addr list ${CLIENT_IB_IF} | awk '{print $4}' | cut -d/ -f1`
if [ -z "$CLIENT_IP" ]; then
	echo "Could not extract IP address from interface ${CLIENT_IB_IF}"
	exit 1
fi

# check whether swapping is enabled and if not then enable
# the swap partition appropriately.
if [ -z `swapon -s` ]; then
	sudo swapon ${SWAP_PARTITION}
fi

CGROUP_NAME=stretch
CGROUP_MEM_BYTES=$((300*1024*1024)) # 300 MB
# check whether cgroup exists and create it otherwise
if [ ! -d "/sys/fs/cgroup/memory/${CGROUP_NAME}" ]; then
	echo "Cgroup memory:${CGROUP_NAME} does not exist."
	sudo cgcreate -a $USER:$USER -g:memory:${CGROUP_NAME}
	echo ${CGROUP_MEM_BYTES} > /sys/fs/cgroup/memory/${CGROUP_NAME}/memory.limit_in_bytes
fi

# build benchmark cpp program
make clean && make

# benchmark application using the classic swap using disk
res_dir=${RESULTS_DIR}/diskswap
mkdir -p ${res_dir}

sar -B 1 20 > /tmp/diskswap_bw.out &
sudo cgexec -g memory:${CGROUP_NAME} ./page_strided 3500
sleep 2
tail -n +3 /tmp/diskswap_bw.out | awk '{print $2,$3,$5}' | tr -s '[:blank:]' ',' > ${res_dir}/bw.csv

pushd ../../swapmon_fs
# build module
make clean && make
MODULE_FILENAME=`modinfo -F filename *.ko`
# insert mcswap module
sudo insmod ${MODULE_FILENAME} cip=${CLIENT_IP} \
	endpoint=${SERVER_IP}:${SERVER_PORT} enable_async_mode=1 enable_poll_mode=1
if [ $? -ne 0 ]; then
	echo "error: couldn't insert module"
fi
popd

# benchmark application using mcswap
res_dir=${RESULTS_DIR}/mcswap
mkdir -p ${res_dir}

sar -B 1 10 > /tmp/mcswap_bw.out &
sudo cgexec -g memory:${CGROUP_NAME} ./page_strided 3500
sleep 2
tail -n +3 /tmp/mcswap_bw.out | awk '{print $2,$3,$5}' | tr -s '[:blank:]' ',' > ${res_dir}/bw.csv
