# Running Microbenchmarks in simpleTransactionRep.cc

This guide explains how to run two microbenchmark functions from `simpleTransactionRep.cc`:

1. `microbench_15pct_mr_high_contention`
2. `microbench_15pct_mr_high_contention_with_retry_and_percentiles`

---

## Prerequisites

- Build environment configured for Mako
- Two terminal windows available
- Root/sudo access for `tc` commands

---

## Running `microbench_15pct_mr_high_contention`

### Step 1: Configure the Test

In `simpleTransactionRep.cc`, locate the `run_worker_tests` function and ensure only these function calls are uncommented:

```cpp
worker->populate_keys(1000);
worker->microbench_15pct_mr_high_contention(1000);
```

Comment out all other function calls in `run_worker_tests`.

### Step 2: Set Network Latency Using TC

Add 100ms latency to simulate remote shard communication:

```bash
sudo tc qdisc add dev lo root netem delay 100ms
```

To remove the latency later:

```bash
sudo tc qdisc del dev lo root
```

### Step 3: Build the Project

```bash
make -j16
```

### Step 4: Run the Benchmark

Open **two terminals** and run the following commands:

**Terminal 1 (Shard 0):**
```bash
./build/simpleTransactionRep 2 0 16 localhost 0 2>&1 | tee /tmp/shard0.log
```

**Terminal 2 (Shard 1):**
```bash
./build/simpleTransactionRep 2 1 16 localhost 0 2>&1 | tee /tmp/shard1.log
```

### Step 5: Check Latency Results

```bash
grep SUMMARY /tmp/shard0.log
```

---

## Running with Sentinel (MR Transaction Prioritization)

To enable Sentinel prioritization for MR transactions, modify **line 630** in `simpleTransactionRep.cc`:

**Before:**
```cpp
void *txn = db->new_txn(0, arena, txn_buf(), abstract_db::HINT_DEFAULT, false/*is_mr*/);
```

**After:**
```cpp
void *txn = db->new_txn(0, arena, txn_buf(), abstract_db::HINT_DEFAULT, true/*is_mr*/);
```

Then rebuild and run:
```bash
make -j16
./build/simpleTransactionRep 2 0 16 localhost 0 2>&1 | tee /tmp/shard0.log
./build/simpleTransactionRep 2 1 16 localhost 0 2>&1 | tee /tmp/shard1.log
```

---

## Running `microbench_15pct_mr_high_contention_with_retry_and_percentiles`

### Step 1: Configure the Test

In `run_worker_tests`, comment out the first function and uncomment the retry variant:

```cpp
worker->populate_keys(1000);
// worker->microbench_15pct_mr_high_contention(1000);
worker->microbench_15pct_mr_high_contention_with_retry_and_percentiles(1000);
```

### Step 2: Enable Sentinel

To enable Sentinel prioritization, modify **line 716** in `simpleTransactionRep.cc`:

**Before:**
```cpp
void *txn = db->new_txn(0, arena, txn_buf(), abstract_db::HINT_DEFAULT, false/*is_mr*/);
```

**After:**
```cpp
void *txn = db->new_txn(0, arena, txn_buf(), abstract_db::HINT_DEFAULT, true/*is_mr*/);
```

### Step 3: Build and Run

Follow the same build and run steps as above (Steps 2-4 from the first function).

### Step 4: Check Percentile Latency Results

```bash
grep P999 /tmp/shard0.log
```

This displays the P999 latency for each worker.

---

## Configuration Options

### Number of Hot Keys

Modify the `max_hot` variable inside each function:

```cpp
int max_hot = 100;  // Change this value as needed
```

### Number of Transactions

Pass a different value when calling the function in `run_worker_tests`:

```cpp
worker->microbench_15pct_mr_high_contention(5000);  // Run 5000 transactions
```

### Number of Threads

Change the thread count via CLI argument (third parameter):

```bash
# 8 threads instead of 16
./build/simpleTransactionRep 2 0 8 localhost 0 2>&1 | tee /tmp/shard0.log
```

---

## Command-Line Arguments Reference

```
./build/simpleTransactionRep <nshards> <shardIdx> <nthreads> <paxos_proc_name> <is_replicated>
```

| Argument | Description | Example |
|----------|-------------|---------|
| `nshards` | Total number of shards | `2` |
| `shardIdx` | Current shard index (0-based) | `0` or `1` |
| `nthreads` | Number of worker threads | `16` |
| `paxos_proc_name` | Paxos process name | `localhost` |
| `is_replicated` | Enable replication (0 or 1) | `0` |

---