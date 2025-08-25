#!/bin/bash
#sudo cgdelete -g cpuset:/cpulimit 2>/dev/null || true
#sudo cgcreate -t $USER:$USER -a $USER:$USER -g cpuset:/cpulimit
set -xv
nshard=$1
shard=$2
trd=$3
let up=trd+3
cluster=${4:-localhost}
#sudo cgset -r cpuset.mems=0 cpulimit
#sudo cgset -r cpuset.cpus=0-$up cpulimit
mkdir -p results
path=$(pwd)/src/mako

# sudo gdb --args 
# sudo strace -f -c
# sudo gdb --args cgexec -g cpuset:cpulimit
./build/dbtest \
                --num-threads $trd  \
                --is_micro \
                --shard-index $shard --shard-config $path/config/local-shards$nshard-warehouses$trd.yml \
                -F config/1leader_2followers/paxos$trd\_shardidx$shard.yml -F config/occ_paxos.yml \
                -P $cluster 
