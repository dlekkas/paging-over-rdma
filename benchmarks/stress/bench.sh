#!/bin/bash

SWAP_PARTITION=/dev/nvme0n1p3
CGROUP_NAME=stretch
CGROUP_MEM_BYTES=$((300*1024*1024)) # 300 MB
RESULTS_DIR=./results

if [ "$#" -ne 2 ]; then
	echo "Usage: ./bench.sh <working-set MBs> <n-reps>"
	exit 1
fi
n_mbs=$1
n_reps=$2

# check whether swapping is enabled and if not then enable
# the swap partition appropriately.
if [ -z `swapon -s` ]; then
	echo "No swap areas are enabled in the system."
	sudo swapon ${SWAP_PARTITION}
fi

# check whether cgroup exists and create it otherwise
if [ ! -d "/sys/fs/cgroup/memory/${CGROUP_NAME}" ]; then
	echo "Cgroup memory:${CGROUP_NAME} does not exist."
	sudo cgcreate -a $USER:$USER -g:memory:${CGROUP_NAME}
	echo ${CGROUP_MEM_BYTES} > /sys/fs/cgroup/memory/${CGROUP_NAME}/memory.limit_in_bytes
fi

# build benchmark cpp program
make

mkdir -p ${RESULTS_DIR}
ts=`date +"%d%m.%H%M%S"`
res_file=${RESULTS_DIR}/exec_times_${n_mbs}_${ts}.raw
for i in $( seq 1 ${n_res} ); do
	sudo cgexec -g memory:${CGROUP_NAME} ./example ${n_mbs} >> ${res_file}
done

