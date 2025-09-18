#ifndef MAKO_ROCKSDB_PERSISTENCE_H
#define MAKO_ROCKSDB_PERSISTENCE_H

#include <memory>
#include <string>
#include <queue>
#include <thread>
#include <vector>
#include <functional>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <future>
#include <unordered_map>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>

namespace mako {

struct PersistRequest {
    std::string key;
    std::string value;
    std::function<void(bool)> callback;
    std::promise<bool> promise;
    size_t size{0};  // For debugging
};

class RocksDBPersistence {
public:
    static RocksDBPersistence& getInstance();

    bool initialize(const std::string& db_path, size_t num_threads = 8);
    void shutdown();

    std::future<bool> persistAsync(const char* data, size_t size,
                                   uint32_t shard_id, uint32_t partition_id,
                                   std::function<void(bool)> callback = nullptr);

    std::string generateKey(uint32_t shard_id, uint32_t partition_id,
                           uint32_t epoch, uint64_t seq_num);

    uint32_t getCurrentEpoch() const { return current_epoch_.load(); }
    void setEpoch(uint32_t epoch) { current_epoch_.store(epoch); }

    size_t getPendingWrites() const { return pending_writes_.load(); }

    bool flushAll();

private:
    RocksDBPersistence();
    ~RocksDBPersistence();

    RocksDBPersistence(const RocksDBPersistence&) = delete;
    RocksDBPersistence& operator=(const RocksDBPersistence&) = delete;

    void workerThread();
    uint64_t getNextSequenceNumber(uint32_t partition_id);

    std::unique_ptr<rocksdb::DB> db_;
    rocksdb::Options options_;
    rocksdb::WriteOptions write_options_;

    std::queue<std::unique_ptr<PersistRequest>> request_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;

    std::vector<std::thread> worker_threads_;
    std::atomic<bool> shutdown_flag_{false};
    std::atomic<size_t> pending_writes_{0};

    std::atomic<uint32_t> current_epoch_{0};

    std::mutex seq_mutex_;
    std::unordered_map<uint32_t, std::atomic<uint64_t>> sequence_numbers_;

    bool initialized_{false};
};

} // namespace mako

#endif // MAKO_ROCKSDB_PERSISTENCE_H