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

ts=`date +%Y-%m-%dT%H-%M-%S`
res_dir=${RESULTS_DIR}/${ts}
mkdir -p ${res_dir}
res_file=${res_dir}/app_exec_times_${n_mbs}.raw
for i in $( seq 1 ${n_reps} ); do
	sudo cgexec -g memory:${CGROUP_NAME} ./example ${n_mbs} >> ${res_file}
done

pushd ${res_dir}
sudo cp /sys/kernel/debug/mcswap/store_measure_us store_latencies
sudo cp /sys/kernel/debug/mcswap/load_measure_us load_latencies
sudo chown $USER:$USER store_latencies load_latencies
scp dilekkas@lenovo.inf.ethz.ch:~/paging-over-rdma/mem_server/times.txt ack_latencies
popd
python3 ../measure.py --ack-latencies-file ${res_dir}/ack_latencies \
	--store-latencies-file ${res_dir}/store_latencies \
	--load-latencies-file ${res_dir}/load_latencies > ${res_dir}/statistics

make clean
