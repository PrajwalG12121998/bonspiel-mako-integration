#pragma once
#include <rusty/rusty.hpp>

#include <unordered_map>
#include <mutex>

#include "misc/marshal.hpp"
#include "reactor/epoll_wrapper.h"
#include "reactor/reactor.h"

// External safety annotations for system functions used in this module
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
//   std::lock_guard: [safe, (std::mutex&) -> void]
//   std::unique_lock: [safe, (std::mutex&) -> void]
//   std::chrono::duration: [safe, (double) -> void]
//   std::function: [safe, (auto) -> void]
//   std::vector::push_back: [safe, (auto) -> void]
//   rrr::Log::error: [unsafe]
//   Log_error: [unsafe]
// }

namespace rrr {

class Future;
// @unsafe - Forward declaration of Client class
class Client;

// @safe - Simple attribute struct for Future callbacks
struct FutureAttr {
    FutureAttr(const std::function<void(Future*)>& cb = std::function<void(Future*)>()) : callback(cb) { }

    // callback should be fast, otherwise it hurts rpc performance
    std::function<void(Future*)> callback;
};

// @safe - Thread-safe future for async RPC results with rusty synchronization primitives
class Future: public RefCounted {
    friend class Client;

    struct State {
        bool ready = false;
        bool timed_out = false;
    };

    i64 xid_;
    i32 error_code_;

    FutureAttr attr_;
    Marshal reply_;

    rusty::Mutex<State> state_;
    rusty::Condvar ready_cond_;
    std::mutex condvar_m_; // rusty::Condvar requires std::mutex for wait operations

    // @unsafe - Notifies waiters using rusty::Condvar (low-level sync operation)
    void notify_ready();

protected:

    // protected destructor as required by RefCounted.
    // @safe - RAII destructors handle cleanup automatically
    ~Future() = default;

public:

    // @safe - Default initialization with RAII primitives
    Future(i64 xid, const FutureAttr& attr = FutureAttr())
            : xid_(xid), error_code_(0), attr_(attr), state_(State{}) {
        // RAII: state_, condvar_m_, and ready_cond_ initialize themselves
    }

    // @safe - Uses rusty::Mutex for thread-safe access
    bool ready() {
        auto guard = state_.lock();
        return guard->ready;
    }

    // @unsafe - Blocks on rusty::Condvar (low-level sync operation)
    void wait();

    // @unsafe - Timed wait using rusty::Condvar (low-level sync operation)
    void timed_wait(double sec);

    // @unsafe - Thread-safe timed_out check (non-blocking)
    // SAFETY: Protected by mutex
    bool timed_out() {
        auto guard = state_.lock();
        return guard->timed_out;
    }

    // @safe - Returns reference to reply with lifetime tied to Future
    // @lifetime: (&'a) -> &'a
    Marshal& get_reply() {
        // @unsafe {
        wait();
        // }
        return reply_;
    }

    // @unsafe - Calls unsafe wait()
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
    // @unsafe - Adds future to group (calls Log_error)
    void add(Future* f) {
        if (f == nullptr) {
            // @unsafe {
            Log_error("Invalid Future object passed to FutureGroup!");
            // }
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

// @unsafe - RPC client with socket management and marshaling using Arc
// SAFETY: Uses mutable SpinLocks for thread-safe interior mutability
// Client is accessed from multiple threads (main + PollThreadWorker), so SpinLocks provide synchronization
// MIGRATED: Now uses rusty::Arc<Client> with explicit weak self-reference instead of shared_from_this()
class Client: public Pollable {
    rusty::RefCell<Marshal> in_;
    rusty::RefCell<Marshal> out_;

    /**
     * Shared Arc to PollThreadWorker - thread-safe access
     */
    rusty::Arc<PollThreadWorker> poll_thread_worker_;

    // Weak self-reference for registration with poll thread worker
    // Initialized by set_weak_self() after Arc creation
    rusty::RefCell<rusty::sync::Weak<Client>> weak_self_;

    // Interior mutability for use with Arc (const methods need to modify state)
    rusty::Cell<int> sock_;
    enum {
        NEW, CONNECTED, CLOSED
    };
    rusty::Cell<int> status_;

    rusty::RefCell<rusty::Option<rusty::Box<Marshal::bookmark>>> bmark_;

    rusty::RefCell<Counter> xid_counter_;
    rusty::RefCell<std::unordered_map<i64, Future*>> pending_fu_;

    mutable SpinLock pending_fu_l_;
    mutable SpinLock out_l_;

    // @unsafe - Cancels all pending futures
    // SAFETY: Protected by spinlock
    void invalidate_pending_futures() const;

public:

    // @unsafe - Cleanup destructor
    // SAFETY: Ensures all futures are invalidated
    virtual ~Client() {
        invalidate_pending_futures();
    }


    Client(rusty::Arc<PollThreadWorker> poll_thread_worker):
        in_(),              // Default-constructs RefCell<Marshal>
        out_(),             // Default-constructs RefCell<Marshal>
        poll_thread_worker_(poll_thread_worker),
        weak_self_(),       // Default-constructs RefCell<Weak<Client>>
        sock_(-1),
        status_(NEW),
        bmark_(),           // Default-constructs RefCell<Option<Box<bookmark>>>
        xid_counter_(),     // Default-constructs RefCell<Counter>
        pending_fu_(),      // Default-constructs RefCell<map>
        pending_fu_l_(),    // Default-constructs mutable SpinLock
        out_l_() { }        // Default-constructs mutable SpinLock

    // Factory method to create Client with Arc
    // @unsafe - Returns Arc<Client> with explicit reference counting
    // SAFETY: Arc provides thread-safe reference counting with polymorphism support
    static rusty::Arc<Client> create(rusty::Arc<PollThreadWorker> poll_thread_worker) {
        auto client = rusty::Arc<Client>::make(poll_thread_worker);
        // Initialize weak self-reference for poll thread registration
        // weak_self_ is mutable, so no const_cast needed
        *client->weak_self_.borrow_mut() = client;
        return client;
    }

    // Set weak self-reference (alternative to factory if Arc created elsewhere)
    void set_weak_self(const rusty::Arc<Client>& self) {
        *weak_self_.borrow_mut() = self;
    }

    /**
     * Start a new request. Must be paired with end_request(), even if nullptr returned.
     *
     * The request packet format is: <size> <xid> <rpc_id> <arg1> <arg2> ... <argN>
     */
    // @unsafe - Begins RPC request with marshaling
    // SAFETY: Protected by spinlock, returns refcounted Future
    Future* begin_request(i32 rpc_id, const FutureAttr& attr = FutureAttr()) const;

    // @unsafe - Completes request packet
    // SAFETY: Must be called after begin_request
    void end_request() const;

    // @unsafe - Marshals data into output buffer
    // SAFETY: Protected by RefCell borrow checks
    // @lifetime: (&'a, const T&) -> &'a
    template<class T>
    const Client& operator <<(const T& v) const {
        if (status_.get() == CONNECTED) {
            *this->out_.borrow_mut() << v;
        }
        return *this;
    }

    // NOTE: this function is used *internally* by Python extension
    // @unsafe - Marshals data from another Marshal
    // SAFETY: Protected by RefCell borrow checks
    // @lifetime: (&'a, Marshal&) -> &'a
    const Client& operator <<(Marshal& m) const {
        if (status_.get() == CONNECTED) {
            this->out_.borrow_mut()->read_from_marshal(m, m.content_size());
        }
        return *this;
    }

    // @unsafe - Establishes TCP connection
    // SAFETY: Proper socket creation and cleanup on failure
    int connect(const char* addr) const;

    // reentrant, could be called multiple times
    // @unsafe - Closes socket and cleans up
    // SAFETY: Idempotent, properly invalidates futures
    void close() const;

    int fd() const {
        return sock_.get();
    }

    // @unsafe - Returns current poll mode based on output buffer
    // SAFETY: Uses RefCell borrow operations
    int poll_mode() const;
    // @unsafe - Processes incoming data
    // SAFETY: Protected by spinlock for pending futures
    void handle_read();
    // @unsafe - Sends buffered data
    // SAFETY: Protected by output spinlock
    void handle_write();
    // @unsafe - Error handler that closes connection
    void handle_error();

};

// @safe - Thread-safe pool of client connections using Arc
// MIGRATED: Now uses rusty::Arc<Client> for cached connections
class ClientPool: public NoCopy {
    rrr::Rand rand_;

    // owns a shared reference to PollThreadWorker
    rusty::Arc<rrr::PollThreadWorker> poll_thread_worker_;

    // guard cache_
    SpinLock l_;
    // @safe - Uses rusty::Arc<Client> for thread-safe reference counting
    // SAFETY: Arc provides thread-safe reference counting with polymorphism support
    std::map<std::string, std::vector<rusty::Arc<Client>>> cache_;
    int parallel_connections_;

public:

    // @safe - Creates pool with optional PollThreadWorker
    // SAFETY: Shared ownership of PollThreadWorker
    ClientPool(rusty::Arc<rrr::PollThreadWorker> poll_thread_worker = rusty::Arc<rrr::PollThreadWorker>(), int parallel_connections = 1);
    // @safe - Closes all cached connections
    // SAFETY: Properly releases all clients and PollThreadWorker via Arc
    ~ClientPool();

    // return cached client connection
    // on error, return empty Arc
    // @safe - Gets or creates client connection, returns Arc<Client>
    // SAFETY: Protected by spinlock, handles connection failures, Arc for thread-safe reference counting
    rusty::Arc<rrr::Client> get_client(const std::string& addr);

};

}
