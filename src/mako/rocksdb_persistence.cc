#include "rocksdb_persistence.h"
#include <sstream>
#include <iomanip>
#include <chrono>
#include <algorithm>
#include <rocksdb/write_batch.h>
#include "../deptran/s_main.h"

namespace mako {

RocksDBPersistence::RocksDBPersistence() {}

RocksDBPersistence::~RocksDBPersistence() {
    shutdown();
}

RocksDBPersistence& RocksDBPersistence::getInstance() {
    static RocksDBPersistence instance;
    return instance;
}

bool RocksDBPersistence::initialize(const std::string& db_path, size_t num_threads) {
    if (initialized_) {
        return true;
    }

    options_.create_if_missing = true;
    options_.max_open_files = 1024;  // Good for concurrency
    options_.write_buffer_size = 256 * 1024 * 1024;  // 256MB per buffer for large logs
    options_.max_write_buffer_number = 6;  // More buffers to prevent stalls
    options_.min_write_buffer_number_to_merge = 2;
    options_.target_file_size_base = 256 * 1024 * 1024;  // 256MB files
    options_.compression = rocksdb::kNoCompression;
    options_.max_background_jobs = 8;  // More background threads
    options_.max_background_compactions = 6;
    options_.max_background_flushes = 4;

    // Optimize for large values
    options_.max_bytes_for_level_base = 1024 * 1024 * 1024;  // 1GB
    options_.level0_slowdown_writes_trigger = 30;
    options_.level0_stop_writes_trigger = 40;

    // Better parallelism
    options_.allow_concurrent_memtable_write = true;
    options_.enable_write_thread_adaptive_yield = true;
    options_.enable_pipelined_write = true;  // Pipeline writes for better performance
    options_.use_direct_io_for_flush_and_compaction = false;  // Normal I/O

    // Memory optimization
    options_.memtable_huge_page_size = 2 * 1024 * 1024;  // 2MB huge pages
    options_.max_successive_merges = 0;

    // Sync periodically to avoid large bursts
    options_.bytes_per_sync = 2 * 1024 * 1024;  // 2MB
    options_.wal_bytes_per_sync = 2 * 1024 * 1024;  // 2MB

    write_options_.sync = false;
    write_options_.disableWAL = false;
    write_options_.no_slowdown = true;  // Don't slow down writes

    rocksdb::DB* db_raw;
    rocksdb::Status status = rocksdb::DB::Open(options_, db_path, &db_raw);
    if (!status.ok()) {
        fprintf(stderr, "Failed to open RocksDB: %s\n", status.ToString().c_str());
        return false;
    }
    db_.reset(db_raw);

    // Initialize epoch to a default value
    // Will be overridden by actual epoch from get_epoch() when used in production
    current_epoch_.store(1);

    shutdown_flag_ = false;
    // Use the requested number of worker threads
    for (size_t i = 0; i < num_threads; ++i) {
        worker_threads_.emplace_back(&RocksDBPersistence::workerThread, this);
    }

    initialized_ = true;
    // RocksDB persistence initialized
    return true;
}

void RocksDBPersistence::shutdown() {
    if (!initialized_) {
        return;
    }

    shutdown_flag_ = true;
    queue_cv_.notify_all();

    for (auto& thread : worker_threads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    worker_threads_.clear();

    while (!request_queue_.empty()) {
        auto req = std::move(request_queue_.front());
        request_queue_.pop();
        if (req->callback) {
            req->callback(false);
        }
        req->promise.set_value(false);
    }

    if (db_) {
        db_->FlushWAL(true);
        db_.reset();
    }

    initialized_ = false;
    // RocksDB persistence shutdown complete
}

std::string RocksDBPersistence::generateKey(uint32_t shard_id, uint32_t partition_id,
                                           uint32_t epoch, uint64_t seq_num) {
    std::stringstream ss;
    ss << std::setfill('0')
       << std::setw(3) << shard_id << ":"
       << std::setw(3) << partition_id << ":"
       << std::setw(8) << epoch << ":"
       << std::setw(16) << seq_num;
    return ss.str();
}

uint32_t RocksDBPersistence::getCurrentEpoch() const {
    return current_epoch_.load();
}

void RocksDBPersistence::setEpoch(uint32_t epoch) {
    current_epoch_.store(epoch);
}

uint64_t RocksDBPersistence::getNextSequenceNumber(uint32_t partition_id) {
    std::lock_guard<std::mutex> lock(seq_mutex_);
    auto it = sequence_numbers_.find(partition_id);
    if (it == sequence_numbers_.end()) {
        sequence_numbers_[partition_id].store(1);  // Next one will be 1
        return 0;  // Return 0 for the first sequence
    }
    return it->second.fetch_add(1);  // Fetch current value and then increment
}

// Legacy interface for backward compatibility - defaults to ordered callbacks
std::future<bool> RocksDBPersistence::persistAsync(const char* data, size_t size,
                                                   uint32_t shard_id, uint32_t partition_id,
                                                   std::function<void(bool)> callback) {
    return persistAsync(data, size, shard_id, partition_id, callback, true);  // Default to ordered
}

// New interface with ordering control
std::future<bool> RocksDBPersistence::persistAsync(const char* data, size_t size,
                                                   uint32_t shard_id, uint32_t partition_id,
                                                   std::function<void(bool)> callback,
                                                   bool require_ordering) {
    if (!initialized_) {
        // Not initialized - this is normal for followers/learners
        // Return success without doing anything
        std::promise<bool> success_promise;
        auto future = success_promise.get_future();
        success_promise.set_value(true);
        if (callback) {
            callback(true);
        }
        return future;
    }

    // Check queue size to prevent unbounded growth
    size_t queue_size = pending_writes_.load();
    if (queue_size > 10000) {  // Backpressure at 10k pending writes
        fprintf(stderr, "RocksDB queue overflow: %zu pending writes, rejecting new request (size=%zu)\n",
                queue_size, size);
        std::promise<bool> error_promise;
        auto future = error_promise.get_future();
        error_promise.set_value(false);
        if (callback) {
            callback(false);
        }
        return future;
    }

    auto req = std::make_unique<PersistRequest>();

    uint32_t epoch = current_epoch_.load();
    if (epoch == 0) {
        // If epoch is 0, try to get it from the system, but only in production
        // In tests, we'll use a default value set during initialization
        epoch = 1;
        current_epoch_.store(epoch);
    }

    uint64_t seq_num = getNextSequenceNumber(partition_id);
    req->key = generateKey(shard_id, partition_id, epoch, seq_num);
    // For large logs, avoid copying if possible
    req->value.reserve(size);  // Pre-allocate to avoid reallocation
    req->value.assign(data, size);
    req->partition_id = partition_id;
    req->sequence_number = seq_num;
    req->require_ordering = require_ordering;
    req->size = size;  // Store size for debugging

    // If ordering is required, store callback in partition state
    if (require_ordering && callback) {
        std::lock_guard<std::mutex> state_lock(partition_states_mutex_);
        auto& state = partition_states_[partition_id];
        if (!state) {
            state = std::make_unique<PartitionState>();
        }

        std::lock_guard<std::mutex> lock(state->state_mutex);
        state->pending_callbacks[seq_num] = callback;
        state->highest_queued_seq = std::max(state->highest_queued_seq.load(), seq_num);
        req->callback = nullptr;  // Will be called from processOrderedCallbacks
    } else {
        req->callback = callback;
    }

    auto future = req->promise.get_future();

    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        request_queue_.push(std::move(req));
        pending_writes_.fetch_add(1);
    }
    queue_cv_.notify_one();

    return future;
}

void RocksDBPersistence::workerThread() {
    std::vector<std::unique_ptr<PersistRequest>> batch;
    const size_t MAX_BATCH_SIZE = 100;  // Process up to 100 writes at once
    const size_t MAX_BATCH_BYTES = 10 * 1024 * 1024;  // 10MB max batch size

    while (!shutdown_flag_) {
        batch.clear();
        size_t batch_bytes = 0;

        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_cv_.wait(lock, [this] {
                return !request_queue_.empty() || shutdown_flag_;
            });

            if (shutdown_flag_ && request_queue_.empty()) {
                break;
            }

            // Collect multiple requests into a batch
            while (!request_queue_.empty() &&
                   batch.size() < MAX_BATCH_SIZE &&
                   batch_bytes < MAX_BATCH_BYTES) {
                auto& req = request_queue_.front();
                batch_bytes += req->value.size();
                batch.push_back(std::move(request_queue_.front()));
                request_queue_.pop();
            }
        }

        if (!batch.empty()) {
            auto start_time = std::chrono::high_resolution_clock::now();

            // Use WriteBatch for better performance
            rocksdb::WriteBatch write_batch;
            for (const auto& req : batch) {
                write_batch.Put(req->key, req->value);
            }

            rocksdb::Status status = db_->Write(write_options_, &write_batch);
            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

            bool success = status.ok();

            // Process callbacks and promises for all requests in batch
            for (auto& req : batch) {
                if (req->require_ordering) {
                    // Handle ordered callback
                    handlePersistComplete(req->partition_id, req->sequence_number,
                                        nullptr, success);  // Callback already stored in partition state
                } else if (req->callback) {
                    req->callback(success);
                }
                req->promise.set_value(success);
                pending_writes_.fetch_sub(1);
            }

            if (!success) {
                fprintf(stderr, "RocksDB batch write failed (%zu requests, %zu bytes, duration=%ldms): %s\n",
                       batch.size(), batch_bytes, duration.count(), status.ToString().c_str());
            } else if (batch_bytes > 100000) {  // Log large batches
                fprintf(stderr, "RocksDB batch write success: %zu requests, %zu bytes, duration=%ldms, pending=%zu\n",
                       batch.size(), batch_bytes, duration.count(), pending_writes_.load());
            }
        }
    }
}

bool RocksDBPersistence::flushAll() {
    if (!db_) {
        return false;
    }

    rocksdb::FlushOptions flush_options;
    flush_options.wait = true;
    rocksdb::Status status = db_->Flush(flush_options);

    if (!status.ok()) {
        fprintf(stderr, "RocksDB flush failed: %s\n", status.ToString().c_str());
        return false;
    }

    status = db_->FlushWAL(true);
    if (!status.ok()) {
        fprintf(stderr, "RocksDB WAL flush failed: %s\n", status.ToString().c_str());
        return false;
    }

    return true;
}

void RocksDBPersistence::handlePersistComplete(uint32_t partition_id, uint64_t sequence_number,
                                              std::function<void(bool)> callback, bool success) {
    std::lock_guard<std::mutex> state_lock(partition_states_mutex_);
    auto it = partition_states_.find(partition_id);
    if (it == partition_states_.end()) {
        // No partition state, just call callback if provided
        if (callback) {
            callback(success);
        }
        return;
    }

    auto& state = it->second;
    std::lock_guard<std::mutex> lock(state->state_mutex);

    // Mark this sequence as persisted
    state->persisted_sequences.insert(sequence_number);
    state->persist_results[sequence_number] = success;

    // Process any callbacks that are now ready
    processOrderedCallbacks(partition_id);
}

void RocksDBPersistence::processOrderedCallbacks(uint32_t partition_id) {
    // Called with partition_states_mutex_ and state->state_mutex held
    auto it = partition_states_.find(partition_id);
    if (it == partition_states_.end()) {
        return;
    }

    auto& state = it->second;
    uint64_t next_seq = state->next_expected_seq.load();

    // Process all callbacks that are ready (all previous sequences persisted)
    while (state->persisted_sequences.count(next_seq) > 0) {
        // This sequence has been persisted
        state->persisted_sequences.erase(next_seq);

        // Get the result for this sequence
        bool success = true;
        auto result_it = state->persist_results.find(next_seq);
        if (result_it != state->persist_results.end()) {
            success = result_it->second;
            state->persist_results.erase(result_it);
        }

        // Find and execute the callback
        auto callback_it = state->pending_callbacks.find(next_seq);
        if (callback_it != state->pending_callbacks.end()) {
            auto callback = callback_it->second;
            state->pending_callbacks.erase(callback_it);

            // Execute callback without holding locks to avoid deadlock
            state->state_mutex.unlock();
            callback(success);
            state->state_mutex.lock();
        }

        // Move to next sequence
        state->next_expected_seq.store(next_seq + 1);
        next_seq++;
    }
}

} // namespace mako