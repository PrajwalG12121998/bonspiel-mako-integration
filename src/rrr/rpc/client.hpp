#pragma once
#include <rusty/arc.hpp>
#include <rusty/option.hpp>

#include <unordered_map>
#include <mutex>
#include <condition_variable>

#include "misc/marshal.hpp"
#include "reactor/epoll_wrapper.h"
#include "reactor/reactor.h"

// External safety annotations for system functions used in this module
// IMPORTANT: All external functions must be marked [unsafe] because:
// - External code is not analyzed/verified by RustyCpp
// - 'unsafe' means programmer-audited (takes responsibility)
// - 'safe' would imply tool verification, which doesn't happen for external code
// @external: {
//   socket: [unsafe, (int, int, int) -> int]
//   connect: [unsafe, (int, const struct sockaddr*, socklen_t) -> int]
//   close: [unsafe, (int) -> int]
//   setsockopt: [unsafe, (int, int, int, const void*, socklen_t) -> int]
//   getaddrinfo: [unsafe, (const char*, const char*, const struct addrinfo*, struct addrinfo**) -> int]
//   freeaddrinfo: [unsafe, (struct addrinfo*) -> void]
//   gai_strerror: [unsafe, (int) -> const char*]
//   memset: [unsafe, (void*, int, size_t) -> void*]
//   strcpy: [unsafe, (char*, const char*) -> char*]
//   std::condition_variable::condition_variable: [unsafe, () -> void]
//   std::unique_lock::unique_lock: [unsafe, (std::mutex&) -> void]
//   std::lock_guard::lock_guard: [unsafe, (std::mutex&) -> void]
//   std::unique_lock::unlock: [unsafe, () -> void]
//   std::condition_variable::wait: [unsafe, (std::unique_lock&, predicate) -> void]
//   std::condition_variable::wait_for: [unsafe, (std::unique_lock&, duration, predicate) -> bool]
//   std::condition_variable::notify_all: [unsafe, () -> void]
//   std::chrono::duration::duration: [unsafe, (double) -> void]
// }

namespace rrr {

class Future;
class Client;

// @safe - Simple attribute struct for Future callbacks
struct FutureAttr {
    FutureAttr(const std::function<void(Future*)>& cb = std::function<void(Future*)>()) : callback(cb) { }

    // callback should be fast, otherwise it hurts rpc performance
    std::function<void(Future*)> callback;
};

// @safe - Thread-safe future for async RPC results with C++ standard library synchronization
class Future: public RefCounted {
    friend class Client;

    i64 xid_;
    i32 error_code_;

    FutureAttr attr_;
    Marshal reply_;

    bool ready_;
    bool timed_out_;
    std::condition_variable ready_cond_;
    std::mutex ready_m_;

    // @safe - Notifies waiters and triggers callbacks with RAII mutex
    // SAFETY: std::mutex provides RAII, callback executed in coroutine
    void notify_ready();

protected:

    // protected destructor as required by RefCounted.
    // @safe - RAII destructors handle cleanup automatically
    // SAFETY: std::mutex and std::condition_variable have proper destructors
    ~Future() = default;

public:

    // @safe - Default initialization with RAII primitives
    // SAFETY: std::mutex and std::condition_variable initialize themselves
    Future(i64 xid, const FutureAttr& attr = FutureAttr())
            : xid_(xid), error_code_(0), attr_(attr), ready_(false), timed_out_(false) {
        // RAII: ready_m_ and ready_cond_ initialize themselves
    }

    // @unsafe - Calls std::lock_guard (external unsafe)
    // SAFETY: Thread-safe RAII lock
    bool ready() {
        std::lock_guard<std::mutex> lock(ready_m_);
        return ready_;
    }

    // wait till reply done
    // @safe - Blocks on condition variable with RAII lock
    // SAFETY: std::unique_lock provides RAII mutex locking, condition_variable is safe
    void wait();

    // @safe - Timed wait with timeout using RAII lock
    // SAFETY: std::unique_lock provides RAII, std::condition_variable::wait_for is safe
    void timed_wait(double sec);

    // @unsafe - Thread-safe timed_out check (non-blocking)
    // SAFETY: Protected by mutex
    bool timed_out() {
        Pthread_mutex_lock(&ready_m_);
        bool t = timed_out_;
        Pthread_mutex_unlock(&ready_m_);
        return t;
    }

    Marshal& get_reply() {
        wait();
        return reply_;
    }

    i32 get_error_code() {
        wait();
        return error_code_;
    }

    // @safe - Null-safe release helper
    static inline void safe_release(Future* fu) {
        if (fu != nullptr) {
            fu->release();
        }
    }
};

// @safe - RAII container for managing multiple futures
class FutureGroup {
private:
    std::vector<Future*> futures_;

public:
    void add(Future* f) {
        if (f == nullptr) {
            Log_error("Invalid Future object passed to FutureGroup!");
            return;
        }
        futures_.push_back(f);
    }

    void wait_all() {
        for (auto& f : futures_) {
            f->wait();
        }
    }

    ~FutureGroup() {
        wait_all();
        for (auto& f : futures_) {
            f->release();
        }
    }
};

// @unsafe - RPC client with socket management and marshaling
// SAFETY: Proper socket lifecycle and thread-safe pending futures
class Client: public Pollable, public std::enable_shared_from_this<Client> {
    Marshal in_, out_;

    /**
     * Shared Arc<Mutex<>> to PollThreadWorker - thread-safe access
     */
    rusty::Arc<PollThreadWorker> poll_thread_worker_;

    int sock_;
    enum {
        NEW, CONNECTED, CLOSED
    } status_;

    rusty::Option<rusty::Box<Marshal::bookmark>> bmark_;

    Counter xid_counter_;
    std::unordered_map<i64, Future*> pending_fu_;

    SpinLock pending_fu_l_;
    SpinLock out_l_;

    // @unsafe - Cancels all pending futures
    // SAFETY: Protected by spinlock
    void invalidate_pending_futures();

public:

    // @unsafe - Cleanup destructor
    // SAFETY: Ensures all futures are invalidated
    virtual ~Client() {
        invalidate_pending_futures();
    }


    Client(rusty::Arc<PollThreadWorker> poll_thread_worker): poll_thread_worker_(poll_thread_worker), sock_(-1), status_(NEW) { }

    // Factory method to create Client with shared_ptr and add to poll_thread_worker
    static std::shared_ptr<Client> create(rusty::Arc<PollThreadWorker> poll_thread_worker) {
        auto client = std::make_shared<Client>(poll_thread_worker);
        // Note: Client is added to poll_thread_worker when connect() is called
        return client;
    }

    /**
     * Start a new request. Must be paired with end_request(), even if nullptr returned.
     *
     * The request packet format is: <size> <xid> <rpc_id> <arg1> <arg2> ... <argN>
     */
    // @unsafe - Begins RPC request with marshaling
    // SAFETY: Protected by spinlock, returns refcounted Future
    Future* begin_request(i32 rpc_id, const FutureAttr& attr = FutureAttr());

    // @unsafe - Completes request packet
    // SAFETY: Must be called after begin_request
    void end_request();

    template<class T>
    Client& operator <<(const T& v) {
        if (status_ == CONNECTED) {
            this->out_ << v;
        }
        return *this;
    }

    // NOTE: this function is used *internally* by Python extension
    Client& operator <<(Marshal& m) {
        if (status_ == CONNECTED) {
            this->out_.read_from_marshal(m, m.content_size());
        }
        return *this;
    }

    // @unsafe - Establishes TCP connection
    // SAFETY: Proper socket creation and cleanup on failure
    int connect(const char* addr);

    // reentrant, could be called multiple times
    // @unsafe - Closes socket and cleans up
    // SAFETY: Idempotent, properly invalidates futures
    void close();

    int fd() {
        return sock_;
    }

    // @safe - Returns current poll mode based on output buffer
    int poll_mode();
    // @unsafe - Processes incoming data
    // SAFETY: Protected by spinlock for pending futures
    void handle_read();
    // @unsafe - Sends buffered data
    // SAFETY: Protected by output spinlock
    void handle_write();
    // @safe - Error handler that closes connection
    void handle_error();

};

// @safe - Thread-safe pool of client connections
class ClientPool: public NoCopy {
    rrr::Rand rand_;

    // owns a shared reference to PollThreadWorker
    rusty::Arc<rrr::PollThreadWorker> poll_thread_worker_;

    // guard cache_
    SpinLock l_;
    std::map<std::string, std::vector<std::shared_ptr<Client>>> cache_;
    int parallel_connections_;

public:

    // @unsafe - Creates pool with optional PollThreadWorker
    // SAFETY: Shared ownership of PollThreadWorker
    ClientPool(rusty::Arc<rrr::PollThreadWorker> poll_thread_worker = rusty::Arc<rrr::PollThreadWorker>(), int parallel_connections = 1);
    // @unsafe - Closes all cached connections
    // SAFETY: Properly releases all clients and PollThreadWorker
    ~ClientPool();

    // return cached client connection
    // on error, return nullptr
    // @unsafe - Gets or creates client connection
    // SAFETY: Protected by spinlock, handles connection failures
    std::shared_ptr<rrr::Client> get_client(const std::string& addr);

};

}
