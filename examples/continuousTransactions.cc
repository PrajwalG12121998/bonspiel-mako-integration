//
// Continuous Transaction Test for Mako Database
// Simple test that continuously executes read/write transactions with statistics
//

#include <iostream>
#include <chrono>
#include <thread>
#include <vector>
#include <atomic>
#include <random>
#include <iomanip>
#include <signal.h>
#include <map>
#include <mako.hh>
#include "examples/common.h"
#include "benchmarks/rpc_setup.h"
#include "../src/mako/spinbarrier.h"
#include "../src/mako/benchmarks/mbta_sharded_ordered_index.hh"

using namespace std;
using namespace mako;

const int MAX_KEYS = 100000;

// Global statistics counters
struct Statistics {
    atomic<uint64_t> total_attempts{0};
    atomic<uint64_t> successful_commits{0};
    atomic<uint64_t> aborts{0};
    atomic<uint64_t> reads{0};
    atomic<uint64_t> writes{0};
    atomic<uint64_t> cross_shard{0};
    atomic<uint64_t> single_shard{0};

    void reset() {
        total_attempts = 0;
        successful_commits = 0;
        aborts = 0;
        reads = 0;
        writes = 0;
        cross_shard = 0;
        single_shard = 0;
    }

    void print(int seconds) {
        uint64_t total = total_attempts.load();
        uint64_t commits = successful_commits.load();
        uint64_t abort_count = aborts.load();
        uint64_t read_count = reads.load();
        uint64_t write_count = writes.load();
        uint64_t cross = cross_shard.load();
        uint64_t single = single_shard.load();

        double abort_rate = total > 0 ? (100.0 * abort_count / total) : 0;
        double cross_rate = total > 0 ? (100.0 * cross / total) : 0;

        printf("[%3ds] TPS: %6lu | Commits: %6lu | Aborts: %5lu (%.1f%%) | R/W: %5lu/%5lu | Cross-shard: %5lu (%.1f%%) | Single-shard: %5lu\n",
               seconds, commits, commits, abort_count, abort_rate,
               read_count, write_count, cross, cross_rate, single);
        fflush(stdout);
    }
};

static Statistics stats;
static volatile bool keep_running = true;

// Signal handler for graceful shutdown
void signal_handler(int sig) {
    keep_running = false;
    printf("\nReceived signal %d, shutting down...\n", sig);
}

class ContinuousWorker {
public:
    ContinuousWorker(abstract_db *db, int worker_id)
        : db_(db), worker_id_(worker_id), gen_(worker_id),
          read_write_dist_(0, 99) {
        txn_obj_buf_.reserve(str_arena::MinStrReserveLength);
        txn_obj_buf_.resize(db->sizeof_txn_object(0));

        home_shard_ = BenchmarkConfig::getInstance().getShardIndex();
        num_shards_ = BenchmarkConfig::getInstance().getNshards();
    }

    void initialize() {
        scoped_db_thread_ctx ctx(db_, false);
    }

    void executeTransactions() {
        mbta_sharded_ordered_index *table = db_->open_sharded_index("customer_0");
        uint64_t key_counter = 0;

        const int MAX_KEYS = 100000;

        while (keep_running) {
            // Decide if this is a read or write (30% writes, 70% reads)
            bool is_write = (read_write_dist_(gen_) < 30);

            stats.total_attempts++;

            void *txn = db_->new_txn(0, arena_, &txn_obj_buf_[0]);
            bool is_cross_shard = false;

            try {
                if (is_write) {
                    // Write transaction
                    stats.writes++;

                    // Generate key
                    uint64_t key_id = key_counter++ % MAX_KEYS;
                    string key = "key_w" + to_string(worker_id_) + "_" + to_string(key_id);
                    string value = Encode("value_" + to_string(worker_id_) + "_" + to_string(key_counter));

                    // Check which shard this key belongs to
                    int key_shard = table->check_shard(key);
                    if (key_shard != home_shard_) {
                        is_cross_shard = true;
                    }

                    table->put(txn, key, value);
                } else {
                    // Read transaction
                    stats.reads++;

                    // Generate key
                    uint64_t read_key_id = gen_() % MAX_KEYS;
                    string key = "key_w" + to_string(worker_id_) + "_" + to_string(read_key_id);

                    // Check which shard this key belongs to
                    int key_shard = table->check_shard(key);
                    if (key_shard != home_shard_) {
                        is_cross_shard = true;
                    }

                    string value;
                    table->get(txn, key, value);
                }

                db_->commit_txn(txn);
                stats.successful_commits++;

                // Track cross-shard vs single-shard after successful commit
                if (is_cross_shard) {
                    stats.cross_shard++;
                } else {
                    stats.single_shard++;
                }

            } catch (abstract_db::abstract_abort_exception &ex) {
                db_->abort_txn(txn);
                stats.aborts++;
            } catch (...) {
                db_->abort_txn(txn);
                stats.aborts++;
            }
        }
    }

private:
    abstract_db *db_;
    int worker_id_;
    int home_shard_;
    int num_shards_;

    str_arena arena_;
    string txn_obj_buf_;

    mt19937 gen_;
    uniform_int_distribution<> read_write_dist_;
};

// Statistics printing thread
void stats_printer_thread() {
    int seconds = 0;
    uint64_t last_total_attempts = 0;
    uint64_t last_successful_commits = 0;
    uint64_t last_aborts = 0;
    uint64_t last_reads = 0;
    uint64_t last_writes = 0;
    uint64_t last_cross_shard = 0;
    uint64_t last_single_shard = 0;

    while (keep_running) {
        this_thread::sleep_for(chrono::seconds(1));
        seconds++;

        // Calculate and print per-second statistics
        uint64_t current_total = stats.total_attempts.load();
        uint64_t current_commits = stats.successful_commits.load();
        uint64_t current_aborts = stats.aborts.load();
        uint64_t current_reads = stats.reads.load();
        uint64_t current_writes = stats.writes.load();
        uint64_t current_cross = stats.cross_shard.load();
        uint64_t current_single = stats.single_shard.load();

        Statistics delta;
        delta.total_attempts = current_total - last_total_attempts;
        delta.successful_commits = current_commits - last_successful_commits;
        delta.aborts = current_aborts - last_aborts;
        delta.reads = current_reads - last_reads;
        delta.writes = current_writes - last_writes;
        delta.cross_shard = current_cross - last_cross_shard;
        delta.single_shard = current_single - last_single_shard;

        delta.print(seconds);

        last_total_attempts = current_total;
        last_successful_commits = current_commits;
        last_aborts = current_aborts;
        last_reads = current_reads;
        last_writes = current_writes;
        last_cross_shard = current_cross;
        last_single_shard = current_single;
    }

    printf("\n--- Final Statistics ---\n");
    fflush(stdout);
    stats.print(seconds);
}

int main(int argc, char **argv) {
    // Set up signal handlers
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    // Parse configuration - simplified parameters similar to simpleTransactionRep
    if (argc != 5 && argc != 6) {
        printf("Usage: %s <nshards> <shardIdx> <nthreads> <paxos_proc_name> [is_replicated]\n", argv[0]);
        printf("Example: %s 2 0 4 localhost 0\n", argv[0]);
        return 1;
    }

    int nshards = stoi(argv[1]);
    int shardIdx = stoi(argv[2]);
    int nthreads = stoi(argv[3]);
    string paxos_proc_name = string(argv[4]);
    int is_replicated = argc > 5 ? stoi(argv[5]) : 0;

    // Build config path
    string config_path = get_current_absolute_path()
            + "../src/mako/config/local-shards" + to_string(nshards)
            + "-warehouses" + to_string(nthreads) + ".yml";
    vector<string> paxos_config_file{
        get_current_absolute_path() + "../config/1leader_2followers/paxos" + to_string(nthreads) + "_shardidx" + to_string(shardIdx) + ".yml",
        get_current_absolute_path() + "../config/occ_paxos.yml"
    };

    auto& benchConfig = BenchmarkConfig::getInstance();
    benchConfig.setNshards(nshards);
    benchConfig.setShardIndex(shardIdx);
    benchConfig.setNthreads(nthreads);
    benchConfig.setPaxosProcName(paxos_proc_name);
    benchConfig.setIsReplicated(is_replicated);

    auto config = new transport::Configuration(config_path);
    benchConfig.setConfig(config);
    benchConfig.setPaxosConfigFile(paxos_config_file);

    init_env();

    printf("=== Continuous Transaction Test ===\n");
    printf("Configuration: 70%% reads, 30%% writes\n");
    printf("Home shard: %d, Total shards: %d, Workers: %d\n",
           shardIdx, nshards, nthreads);
    printf("Note: Cross-shard transactions detected automatically based on key hash\n");
    printf("Press Ctrl+C to stop...\n\n");
    fflush(stdout);

    abstract_db* db = initWithDB();

    if (benchConfig.getLeaderConfig()) {
        // pre-declare sharded tables
        mako::setup_erpc_server();
        mbta_sharded_ordered_index *table = db->open_sharded_index("customer_0");

        map<int, abstract_ordered_index*> open_tables;
        auto *local_table = table->shard_for_index(benchConfig.getShardIndex());
        if (local_table) {
            open_tables[local_table->get_table_id()] = local_table;
        }
        mako::setup_helper(db, ref(open_tables));
    }

    // Wait for all shards to initialize their eRPC servers
    // This is critical for multi-shard setups to avoid connection retry loops
    printf("Waiting for all shards to initialize (10 seconds)...\n");
    fflush(stdout);
    this_thread::sleep_for(chrono::seconds(10));
    printf("Starting transaction execution...\n\n");
    fflush(stdout);

    // Start statistics printer thread
    thread stats_thread(stats_printer_thread);

    // Start worker threads
    vector<thread> worker_threads;
    vector<unique_ptr<ContinuousWorker>> workers;

    spin_barrier barrier_ready(nthreads);
    spin_barrier barrier_start(1);

    for (int i = 0; i < nthreads; i++) {
        workers.emplace_back(make_unique<ContinuousWorker>(db, i));
        worker_threads.emplace_back([&workers, &barrier_ready, &barrier_start, i]() {
            workers[i]->initialize();
            barrier_ready.count_down();
            barrier_start.wait_for();
            workers[i]->executeTransactions();
        });
    }

    // Release workers once every thread has initialized
    barrier_ready.wait_for();
    barrier_start.count_down();

    // Wait for all threads to complete
    for (auto &t : worker_threads) {
        t.join();
    }

    keep_running = false;
    stats_thread.join();

    printf("\nTest completed successfully.\n");
    fflush(stdout);

    delete db;
    return 0;
}