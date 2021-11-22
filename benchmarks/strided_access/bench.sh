#!/bin/bash

SWAP_PARTITION=/dev/nvme0n1p3
SERVER_IP='192.168.1.20'
CLIENT_IP='192.168.1.10'
SERVER_PORT=10000

FASTSWAP_ROOT_DIR=$HOME/fastswap
MCSWAP_ROOT_DIR=$HOME/paging-over-rdma
RESULTS_DIR=./results

n_reps=5

if [ "$#" -ne 1 ]; then
	echo "Usage: ./bench.sh <[mcswap|fastswap|diskswap]>"
	exit 1
fi
system=$1

# check whether swapping is enabled and if not then enable
# the swap partition appropriately.
if [ -z `swapon -s` ]; then
	sudo swapon ${SWAP_PARTITION}
fi

CGROUP_NAME=stretch
CGROUP_MEM_BYTES=$((300*1024*1024)) # 300 MB
# check whether cgroup exists and create it otherwise
if [ ! -d "/sys/fs/cgroup/memory/${CGROUP_NAME}" ]; then
	sudo cgcreate -a $USER:$USER -g:memory:${CGROUP_NAME}
fi
echo ${CGROUP_MEM_BYTES} > /sys/fs/cgroup/memory/${CGROUP_NAME}/memory.limit_in_bytes

# build benchmark cpp program
make clean && make

# check if module is already inserted
lsmod | awk '{print $1}' | grep ${system} &> /dev/null

if [ $? -eq 0 ]; then
	echo "Module ${system} is already inserted."
elif [ "$system" == "mcswap" ]; then
	pushd ${MCSWAP_ROOT_DIR}/swapmon_fs
	# build module
	make clean && make
	sudo insmod mcswap.ko cip=${CLIENT_IP} \
		endpoint=${SERVER_IP}:${SERVER_PORT} \
		enable_async_mode=1 enable_poll_mode=1
	if [ $? -ne 0 ]; then
		echo "error: mcswap module failed to load"
		exit 1
	fi
	popd
elif [ "$system" == "fastswap" ]; then
	pushd ${FASTSWAP_ROOT_DIR}/drivers
	# build module
	make clean && make BACKEND=RDMA
	sudo insmod fastswap_rdma.ko sport=${SERVER_PORT} \
		sip=${SERVER_IP} cip=${CLIENT_IP} nq=8
	sudo insmod fastswap.ko
	if [ $? -ne 0 ]; then
		echo "error: fastswap module failed to load"
		exit 1
	fi
	popd
elif [ "$system" != "diskswap" ]; then
	echo "Usage: ./bench.sh [mcswap|fastswap|diskswap]"
	exit 1
fi

res_dir=${RESULTS_DIR}/${system}
mkdir -p ${res_dir}

res_file=${res_dir}/app_exec_times.csv
for n_mbs in $( seq 360 60 600 ); do
	for i in $( seq 1 ${n_reps} ); do
		exec_time=`sudo cgexec -g memory:${CGROUP_NAME} ./example_strided ${n_mbs}`
		echo "${exec_time},${n_mbs}" >> ${res_file}
	done
done


#pushd ${res_dir}
#sudo cp /sys/kernel/debug/mcswap/store_measure_us store_latencies
#sudo cp /sys/kernel/debug/mcswap/load_measure_us load_latencies
#sudo chown $USER:$USER store_latencies load_latencies
#popd
#python3 ../measure.py \
	#--store-latencies-file ${res_dir}/store_latencies \
	#--load-latencies-file ${res_dir}/load_latencies > ${res_dir}/statistics

#make clean
