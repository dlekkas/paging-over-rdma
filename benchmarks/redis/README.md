## Benchmarking Redis-5.0 in-memory data structure store
### Installing Redis-5.0
First, we need to install Redis in the machine which can be achieved by simply
executing the `install.sh` script. However, a more fine-grained explanation of
installation is given below. We will follow the approach that builds Redis from
source which is safest option to work with multiple setups. The commands below
need to be executed:
```
# create directory to store redis sources
mkdir -p $HOME/.local/opt

# clone redis repo for version 5.0
git clone -b 5.0 https://github.com/redis/redis.git $HOME/.local/opt/redis

# compile source code
pushd $HOME/.local/opt/redis
make

# run tests (optional)
make test

# install binaries and libs to default directory /usr/local
sudo make install

# configure init scripts and config files (useful for prod)
sudo ./utils/install_server.sh
popd
```

### Benchmarking Redis with `redis-benchmark` tool
The default redis installation comes with a `redis-benchmark` utility that
simulates workloads and concurrently runs commands by multiple clients by
sending several queries to the redis server. Since our primary goal is to
force swapping we have to deviate from the default benchmarking values and
make sure that our benchmarking workload is memory-intensive enough. For
reference default workload uses workload of only 3 bytes and probes a single
key. Our options of interest in the `redis-benchmark` tools are the `-r` option
that declares the random keys to be used in our workload, the `-n` option that
specifies the total number of requests to be made and the `-d` option that
defines the data size of SET/GET value in bytes.
Before we generate our workload we need to make sure that Redis is running:
```
redis-cli ping
```
If we get back the answer `PONG` then redis is up and running, otherwise we
have to run the service ourselves. Assuming that the previous steps were used
for the installation we can start the service as follows:
```
sudo /etc/init.d/redis_6379 start
```
The following command generates a relatively lightweight workload:
```
redis-benchmark -t set,get -r 1000000 -n 1000000 -d 1024
```
Even though the above workload won't generate swapping on modern machines, it
will lead to a redis memory usage of approximately 300MB. By playing with the
options `-r` and `-d` we can stress the memory as needed. However, a better way
to force swapping would involve running `redis` service within a cgroup with the
desired memory limitations.

### Running Redis within a cgroup
In order to run `redis` within its own cgroup, we first need to create a cgroup
and constrain its memory. This is achieved by the following commands:
```
sudo cgcreate -a $USER:$USER -g memory:stretch
echo $((300*1024*1024)) > /sys/fs/cgroup/memory/stretch/memory.limit_in_bytes
```
We first need to stop the current running redis instance:
```
sudo /etc/init.d/redis_6379 stop
```
Then, we start the redis service to run within our cgroup:
```
sudo cgexec -g memory:stretch /etc/init.d/redis_6379 start
```
By using the same command that generates a lightweight workload that we used
before we can now cause swapping activity:
```
redis-benchmark -t set,get -r 1000000 -n 1000000 -d 1024
```
