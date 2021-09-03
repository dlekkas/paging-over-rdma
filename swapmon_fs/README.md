## Instructions for swap usage monitoring
First, we want to load the swapmon module which uses the frontswap interface
and monitors and prints the requests in the kernel logs that
this device receives. To achieve that, we first have to disable swapping
because we have to open this device exclusively during the module initialization
phase. This is achieved by the following command:
```
swapoff /dev/nvme0n1p3
```

Afterwards, we compile and insert the `stackbd` module:
```
cd swapmon_fs
make
sudo insmod ./swapmon.ko
```

We now prepare our virtual block device for swapping:
```
sudo mkswap /dev/nvme0n1p3
sudo swapon /dev/nvme0n1p3
```

Finally, we need a way to force swapping through an application. However, since
the system will become extremely slow in case of heavy swapping, we leverage
cgroups (`man 7 cgroups`) to minimize available memory for our application and
impact only the application's performance instead of the whole system. We create
our cgroups as follows:
```
# create cgroup for current user
sudo cgcreate -a $USER:$USER -g:memory:stretch
# restrict available memory to 300MB
echo $((300*1024*1024)) >> /sys/fs/cgroup/memory/stretch/memory.limit_in_bytes
```

Finally, we compile our application and execute it within our recently created
cgroup's context:
```
g++ stress.cpp -o stress
cgexec -g memory:stretch ./stress
```

Monitoring the swap usage can happen either by reading the kernel logs directly
(`tail -f /var/log/syslog`) or through the `dmesg` command.
