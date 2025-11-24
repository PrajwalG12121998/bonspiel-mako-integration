#pragma once
#include <algorithm>
#include <list>
#include <memory>
#include <queue>
#include <set>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <variant>
#include <unistd.h>
#include <rusty/rusty.hpp>
#include <rusty/thread.hpp>
#include <rusty/arc.hpp>
#include <rusty/mutex.hpp>
#include <rusty/sync/mpsc.hpp>
#include "base/misc.hpp"
#include "event.h"
#include "quorum_event.h"
#include "coroutine.h"
#include "epoll_wrapper.h"

// External safety annotations for system functions used in this module
// @external: {
//   pthread_setname_np: [unsafe, (pthread_t, const char*) -> int]
// }

// External safety annotations for STL operations
// @external: {
//   operator!=: [unsafe, (auto, auto) -> bool]
//   operator==: [unsafe, (auto, auto) -> bool]
//   std::*::find: [unsafe, (auto) -> auto]
//   std::*::end: [unsafe, () -> auto]
//   std::make_shared: [unsafe, (auto...) -> std::shared_ptr<auto>]
//   std::shared_ptr::operator*: [unsafe, () -> auto&]
//   std::shared_ptr::operator->: [unsafe, () -> auto*]
//   std::shared_ptr::get: [unsafe, () -> auto*]
//   std::shared_ptr::operator=: [unsafe, (const std::shared_ptr<auto>&) -> std::shared_ptr<auto>&]
//   std::shared_ptr::shared_ptr: [unsafe, (auto...) -> void]
//   std::list::push_back: [unsafe, (auto) -> void]
//   std::vector::push_back: [unsafe, (auto) -> void]
// }

namespace rrr {

using std::make_unique;
using std::make_shared;

class Coroutine;
// TODO for now we depend on the rpc services, fix in the future.
// @unsafe - Thread-safe reactor with thread-local storage and mutable fields for interior mutability
class Reactor {
 public:
  // Default constructor - all fields have default constructors
  Reactor() = default;

  // Delete copy and move constructors (RefCell and Cell are not copyable/movable)
  Reactor(const Reactor&) = delete;
  Reactor& operator=(const Reactor&) = delete;
  Reactor(Reactor&&) = delete;
  Reactor& operator=(Reactor&&) = delete;

  // Returns thread-local reactor instance with single-threaded Rc
  // SAFETY: Thread-local storage, single-threaded access only
  static rusty::Rc<Reactor> GetReactor();
  static thread_local rusty::Option<rusty::Rc<Reactor>> sp_reactor_th_;
  // Thread-local current coroutine with single-threaded Rc
  static thread_local rusty::Option<rusty::Rc<Coroutine>> sp_running_coro_th_;
  /**
   * A reactor needs to keep reference to all coroutines created,
   * in case it is freed by the caller after a yield.
   */
  // Events managed with std::shared_ptr (polymorphism support)
  // Interior mutability for const methods
  mutable std::list<std::shared_ptr<Event>> all_events_{};
  mutable std::list<std::shared_ptr<Event>> waiting_events_{};
  // Coroutines managed with single-threaded Rc
  mutable std::set<rusty::Rc<Coroutine>> coros_{};
  mutable std::vector<rusty::Rc<Coroutine>> available_coros_{};
  mutable std::unordered_map<uint64_t, std::function<void(Event&)>> processors_{};
  mutable std::list<std::shared_ptr<Event>> timeout_events_{};
  mutable bool looping_{false};
  std::thread::id thread_id_{};
#ifdef REUSE_CORO
#define REUSING_CORO (true)
#else
#define REUSING_CORO (false)
#endif

  // Checks and processes timeout events with std::shared_ptr
  void CheckTimeout(std::vector<std::shared_ptr<Event>>&) const;
  /**
   * @param ev. is usually allocated on coroutine stack. memory managed by user.
   */
  // Creates and runs a new coroutine with rusty::Rc ownership
  rusty::Rc<Coroutine> CreateRunCoroutine(std::move_only_function<void()> func) const;
  // Main event loop
  void Loop(bool infinite = false) const;
  // Continues execution of a paused coroutine with rusty::Rc
  void ContinueCoro(rusty::Rc<Coroutine> sp_coro) const;

  ~Reactor() {
//    verify(0);
  }
  friend Event;

  // @unsafe - Creates std::shared_ptr event with perfect forwarding
  // SAFETY: Uses std::shared_ptr for polymorphism support. Lifetime is safe because:
  //   1. shared_ptr is stored in all_events_ list (owned by reactor)
  //   2. Reactor lives for entire program duration
  //   3. Events are never removed from all_events_ until reactor destruction
  // Manual verification required due to template complexity and std::shared_ptr usage
  template <typename Ev, typename... Args>
  static std::shared_ptr<Ev> CreateSpEvent(Args&&... args) {  // @unsafe
    auto sp_ev = std::make_shared<Ev>(args...);
    sp_ev->__debug_creator = 1;
    // TODO push them into a wait queue when they actually wait.
    auto reactor = GetReactor();
    // Rc gives const access, use const_cast for mutation (safe: thread-local, single owner)
    auto& events = const_cast<Reactor&>(*reactor).all_events_;
    events.push_back(sp_ev);
    return sp_ev;
  }

  // @unsafe - Creates event and returns reference to shared_ptr content
  // SAFETY: Returned reference is valid because:
  //   1. Event is created via CreateSpEvent and stored in all_events_
  //   2. all_events_ is never cleared during reactor lifetime
  //   3. Returned reference points to heap-allocated Event managed by shared_ptr
  // Manual verification required: reference lifetime extends beyond function scope
  template <typename Ev, typename... Args>
  static Ev& CreateEvent(Args&&... args) {  // @unsafe
    return *CreateSpEvent<Ev>(args...);
  }
};

// Forward declarations
class PollThread;
class PollThreadWorker;

// =============================================================================
// Channel-based communication between PollThread and PollThreadWorker
// =============================================================================

// Commands sent from PollThread to PollThreadWorker via channel
// Using std::variant for type-safe discriminated union
struct CmdAddPollable { rusty::Arc<Pollable> pollable; };
struct CmdRemovePollable { int fd; };
struct CmdUpdateMode { int fd; int new_mode; Pollable* poll_ptr; };
struct CmdAddJob { rusty::Arc<Job> job; };
struct CmdRemoveJob { rusty::Arc<Job> job; };
struct CmdShutdown {};

using PollCommand = std::variant<
    CmdAddPollable,
    CmdRemovePollable,
    CmdUpdateMode,
    CmdAddJob,
    CmdRemoveJob,
    CmdShutdown
>;

} // namespace rrr

// Mark PollCommand as Send for use with rusty::sync::mpsc channel
namespace rusty {
template<>
struct is_send<rrr::PollCommand> : std::true_type {};
} // namespace rusty

namespace rrr {

// =============================================================================
// PollThreadWorker - Owns all polling state, runs in dedicated thread
// =============================================================================

// Worker class that owns all polling state
// Runs entirely in the spawned thread
// Receives commands from PollThread via mpsc channel
//
// @unsafe - Contains STL containers and uses Epoll system calls
// SAFETY: Despite @unsafe annotation, PollThreadWorker is memory-safe because:
// 1. Single-threaded: Runs only on its dedicated poll thread, no data races
// 2. Ownership: Owns all Pollables via fd_to_pollable_ map
// 3. Lifetime: Worker outlives all Pollables - on shutdown, clears worker
//    references before destruction
// 4. Channel: Cross-thread communication only via thread-safe mpsc channel
class PollThreadWorker {
    friend class PollThread;
    friend class rusty::Rc<PollThreadWorker>;

public:
    // Factory method - creates worker wrapped in Rc with weak_self_ initialized
    static rusty::Rc<PollThreadWorker> create(rusty::sync::mpsc::Receiver<PollCommand> receiver);

    // Constructor is public for Rc::make(), but prefer create() factory
    explicit PollThreadWorker(rusty::sync::mpsc::Receiver<PollCommand> receiver);

    ~PollThreadWorker() = default;

    // Delete copy/move - worker is owned by Rc
    PollThreadWorker(const PollThreadWorker&) = delete;
    PollThreadWorker& operator=(const PollThreadWorker&) = delete;
    PollThreadWorker(PollThreadWorker&&) = delete;
    PollThreadWorker& operator=(PollThreadWorker&&) = delete;

    // Main polling loop - processes epoll events and channel commands
    // Const because Rc gives const access - uses mutable fields for state
    void poll_loop() const;

    // Get weak reference to self (for passing to Pollables)
    rusty::rc::Weak<PollThreadWorker> weak_self() const { return weak_self_; }

    // Direct update_mode for use by handlers running on poll thread
    // This bypasses the channel for better performance
    void update_mode(Pollable& poll, int new_mode) const;

private:
    // For testing: get number of epoll Remove() calls
    int get_remove_count() const { return poll_.remove_count_.load(); }

private:
    // Process incoming commands from channel
    void process_commands() const;

    // Triggers ready jobs in coroutines
    void TriggerJob() const;

    // Internal implementations (no longer need to be thread-safe - single owner)
    // All const because they use mutable fields (single-threaded, no races)
    void do_add_pollable(rusty::Arc<Pollable> sp_poll) const;
    void do_remove_pollable(int fd) const;
    void do_update_mode(int fd, int new_mode, Pollable* poll_ptr) const;
    void do_add_job(rusty::Arc<Job> sp_job) const;
    void do_remove_job(rusty::Arc<Job> sp_job) const;

    // Process deferred removals
    void process_pending_removals() const;

private:
    // MPSC receiver for commands from PollThread
    // Mutable for const poll_loop() - single-threaded, no races
    mutable rusty::sync::mpsc::Receiver<PollCommand> receiver_;

    // Epoll instance
    // Mutable for const poll_loop() - single-threaded, no races
    mutable Epoll poll_;

    // Pollable state - no longer needs Mutex (single owner in worker thread)
    // Mutable for const poll_loop() - single-threaded, no races
    mutable std::unordered_map<int, rusty::Arc<Pollable>> fd_to_pollable_;
    mutable std::unordered_map<int, int> mode_;  // fd -> mode
    mutable std::unordered_set<int> pending_remove_;

    // Jobs - no longer needs Mutex (single owner in worker thread)
    // Mutable for const poll_loop() - single-threaded, no races
    mutable std::set<rusty::Arc<Job>> jobs_;

    // Weak reference to self (set by create() factory)
    mutable rusty::rc::Weak<PollThreadWorker> weak_self_{};

    // Stop flag
    // Mutable for const poll_loop() - single-threaded, no races
    mutable bool stop_ = false;
};

// =============================================================================
// PollThread - Handle for controlling the poll thread
// =============================================================================

// @unsafe - Handle for controlling the poll thread (has mutable fields)
// SAFETY: Despite @unsafe annotation, PollThread is thread-safe because:
// 1. All cross-thread communication via thread-safe mpsc channel
// 2. Mutable fields use proper synchronization (mutex for join_handle_, atomic for shutdown_called_)
class PollThread {
    // Friend Arc to allow make access to private constructor
    friend class rusty::Arc<PollThread>;

private:
    // MPSC sender for commands to worker
    mutable rusty::sync::mpsc::Sender<PollCommand> sender_;

    // Join handle for the thread (Mutex provides interior mutability)
    rusty::Mutex<rusty::Option<rusty::thread::JoinHandle<void>>> join_handle_;

    // Thread ID of the poll thread - used to detect self-join attempts
    // std::atomic for safe cross-thread access (set by spawned thread, read by shutdown())
    mutable std::atomic<std::thread::id> poll_thread_id_{};

    // Track if shutdown was called
    mutable std::atomic<bool> shutdown_called_{false};

    // Private constructor - use create() factory
    explicit PollThread(rusty::sync::mpsc::Sender<PollCommand> sender);

public:
    ~PollThread();

    // Factory method returns Arc<PollThread>
    static rusty::Arc<PollThread> create();

    // Explicit shutdown
    void shutdown() const;

    // Delete copy/move
    PollThread(const PollThread&) = delete;
    PollThread& operator=(const PollThread&) = delete;
    PollThread(PollThread&& other) = delete;
    PollThread& operator=(PollThread&& other) = delete;

    // Send commands to worker via channel
    void add(rusty::Arc<Pollable> poll) const;
    void remove(Pollable& poll) const;
    void update_mode(Pollable& poll, int new_mode) const;
    void add(rusty::Arc<Job> sp_job) const;
    void remove(rusty::Arc<Job> sp_job) const;

    // For testing - NOTE: This won't work with channel design
    // since worker state is not accessible. Return 0 for now.
    int get_remove_count() const { return 0; }
};

} // namespace rrr

// Trait specializations for PollThread
// PollThread is Send + Sync because channel operations are thread-safe
namespace rusty {
template<>
struct is_send<rrr::PollThread> : std::true_type {};

template<>
struct is_sync<rrr::PollThread> : std::true_type {};
} // namespace rusty
