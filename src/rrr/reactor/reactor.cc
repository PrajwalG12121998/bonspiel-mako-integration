
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include "../base/all.hpp"
#include "reactor.h"
#include "coroutine.h"
#include "event.h"
#include "epoll_wrapper.h"

namespace rrr {

thread_local rusty::Rc<Reactor> Reactor::sp_reactor_th_{};
thread_local rusty::Rc<Coroutine> Reactor::sp_running_coro_th_{};

// @safe - Returns current coroutine with single-threaded reference counting
// SAFETY: Returns copy of thread-local Rc - single-threaded, no synchronization needed
rusty::Rc<Coroutine> Coroutine::CurrentCoroutine() {
  // TODO re-enable this verify
//  verify(sp_running_coro_th_);
  return Reactor::sp_running_coro_th_.clone();
}

// @safe - Creates and runs a new coroutine with rusty::Rc ownership
// SAFETY: Reactor manages coroutine lifecycle properly with Rc
rusty::Rc<Coroutine>
Coroutine::CreateRun(std::move_only_function<void()> func) {
  auto reactor_rc = Reactor::GetReactor();
  // Rc gives const access, CreateRunCoroutine is const (safe: thread-local, single owner)
  auto coro = reactor_rc->CreateRunCoroutine(std::move(func));
  // some events might be triggered in the last coroutine.
  return coro;
}

// @safe - Returns thread-local reactor instance, creates if needed
// SAFETY: Thread-local storage with Rc ensures single-threaded access
rusty::Rc<Reactor>
Reactor::GetReactor() {
  if (!sp_reactor_th_) {
    Log_debug("create a coroutine scheduler");
    sp_reactor_th_ = rusty::Rc<Reactor>::make();  // In-place construction
    // Use get_mut() to initialize thread_id_ - safe because we just created it
    const_cast<Reactor&>(*sp_reactor_th_).thread_id_ = std::this_thread::get_id();
  }
  return sp_reactor_th_.clone();
}

/**
 * @param func
 * @return
 */
// @safe - Creates and runs coroutine with rusty::Rc single-threaded reference counting
// SAFETY: Proper lifecycle management with Rc, single-threaded execution
rusty::Rc<Coroutine>
Reactor::CreateRunCoroutine(std::move_only_function<void()> func) const {
  rusty::Rc<Coroutine> sp_coro;
  if (REUSING_CORO && available_coros_.borrow()->size() > 0) {
    //Log_info("Reusing stuff");
    sp_coro = available_coros_.borrow()->back().clone();
    available_coros_.borrow_mut()->pop_back();
    // Rc provides const access, use const_cast to modify (safe: single-threaded)
    auto& coro = const_cast<Coroutine&>(*sp_coro);
    *coro.func_.borrow_mut() = std::move(func);
    // Reset boost_coro_task_ when reusing a recycled coroutine for a new function
    *coro.boost_coro_task_.borrow_mut() = rusty::None;
    coro.status_.set(Coroutine::INIT);
  } else {
    sp_coro = rusty::Rc<Coroutine>::make(std::move(func));
  }

  // Save old coroutine context
  auto sp_old_coro = sp_running_coro_th_;
  sp_running_coro_th_ = sp_coro;

  if (!sp_coro) {
    Log_error("[DEBUG] CreateRunCoroutine: sp_coro is null!");
  }
  verify(sp_coro);
  auto pair = coros_.borrow_mut()->insert(sp_coro);
  if (!pair.second) {
    Log_error("[DEBUG] CreateRunCoroutine: Failed to insert coroutine into coros_ set!");
    Log_error("[DEBUG] coros_ size before insert: %zu", coros_.borrow()->size());
    Log_error("[DEBUG] REUSING_CORO: %d", REUSING_CORO);
  }
  verify(pair.second);
  verify(coros_.borrow()->size() > 0);
  
  sp_coro->Run();
  if (sp_coro->Finished()) {
    coros_.borrow_mut()->erase(sp_coro);
  }
  
  Loop();
  
  // yielded or finished, reset to old coro.
  sp_running_coro_th_ = sp_old_coro;
  return sp_coro;
}

// @safe - Checks timeout events and moves ready ones to ready list with std::shared_ptr
void Reactor::CheckTimeout(std::vector<std::shared_ptr<Event>>& ready_events ) const {
  auto time_now = Time::now(true);
  for (auto it = timeout_events_.borrow_mut()->begin(); it != timeout_events_.borrow_mut()->end();) {
    Event& event = **it;
    auto status = event.status_;
    switch (status) {
      case Event::INIT:
        verify(0);
      case Event::WAIT: {
        const auto &wakeup_time = event.wakeup_time_;
        verify(wakeup_time > 0);
        if (time_now > wakeup_time) {
          if (event.IsReady()) {
            // This is because our event mechanism is not perfect, some events
            // don't get triggered with arbitrary condition change.
            event.status_ = Event::READY;
          } else {
            event.status_ = Event::TIMEOUT;
          }
          ready_events.push_back(*it);
          it = timeout_events_.borrow_mut()->erase(it);
        } else {
          it++;
        }
        break;
      }
      case Event::READY:
      case Event::DONE:
        it = timeout_events_.borrow_mut()->erase(it);
        break;
      default:
        verify(0);
    }
  }

}

//  be careful this could be called from different coroutines.
// @unsafe - Main event loop with complex event processing
// SAFETY: Thread-safe via thread_id verification
void Reactor::Loop(bool infinite) const {
  verify(std::this_thread::get_id() == thread_id_);
  looping_.set(infinite);
  do {
    // Keep processing events until no new ready events are found
    // This fixes the event chain propagation issue
    bool found_ready_events = true;
    while (found_ready_events) {
      found_ready_events = false;
      std::vector<std::shared_ptr<Event>> ready_events;

      // Check waiting events
      auto& events = waiting_events_;
      for (auto it = events.borrow_mut()->begin(); it != events.borrow_mut()->end();) {
        Event& event = **it;
        event.Test();
        if (event.status_ == Event::READY) {
          ready_events.push_back(*it);
          it = events.borrow_mut()->erase(it);
          found_ready_events = true;
        } else if (event.status_ == Event::DONE) {
          it = events.borrow_mut()->erase(it);
        } else {
          it ++;
        }
      }

      CheckTimeout(ready_events);

      // Process ready events
      for (auto& sp_ev: ready_events) {
        Event& event = *sp_ev;
        auto option_coro = event.wp_coro_.upgrade();
        verify(option_coro.is_some());
        auto sp_coro = option_coro.unwrap();
        verify(coros_.borrow()->find(sp_coro) != coros_.borrow()->end());
        if (event.status_ == Event::READY) {
          event.status_ = Event::DONE;
        } else {
          verify(event.status_ == Event::TIMEOUT);
        }
        ContinueCoro(sp_coro);
      }

      // If we're not in infinite mode and found no events, stop inner loop
      if (!infinite && !found_ready_events) {
        break;
      }
    }
  } while (looping_.get());
}

// @safe - Continues execution of paused coroutine with rusty::Rc
// SAFETY: Manages coroutine state transitions properly, single-threaded Rc
void Reactor::ContinueCoro(rusty::Rc<Coroutine> sp_coro) const {
//  verify(!sp_running_coro_th_); // disallow nested coros
  auto sp_old_coro = sp_running_coro_th_.clone();
  sp_running_coro_th_ = sp_coro.clone();
  verify(!sp_running_coro_th_->Finished());
  if (sp_coro->status_.get() == Coroutine::INIT) {
    sp_coro->Run();
  } else {
    // PAUSED or RECYCLED
    sp_running_coro_th_->Continue();
  }
  if (sp_running_coro_th_->Finished()) {
    if (REUSING_CORO) {
      // Rc provides const access, use const_cast to modify (safe: single-threaded)
      const_cast<Coroutine&>(*sp_coro).status_.set(Coroutine::RECYCLED);
      available_coros_.borrow_mut()->push_back(sp_running_coro_th_);
    }
    coros_.borrow_mut()->erase(sp_running_coro_th_);
  }
  sp_running_coro_th_ = sp_old_coro;
}

// TODO PollThreadWorker -> Reactor

// Private constructor - doesn't start thread
PollThreadWorker::PollThreadWorker()
    : poll_(Epoll()),
      l_(rusty::make_box<SpinLock>()),
      fd_to_pollable_(),
      mode_(),
      set_sp_jobs_(),
      pending_remove_(),
      pending_remove_l_(rusty::make_box<SpinLock>()),
      lock_job_(rusty::make_box<SpinLock>()),
      join_handle_(rusty::None),
      stop_flag_(rusty::make_box<std::atomic<bool>>(false)) {
  // Don't start thread here - factory will do it
}

// Factory method creates Arc<PollThreadWorker> and starts thread
rusty::Arc<PollThreadWorker> PollThreadWorker::create() {
  // Create Arc with PollThreadWorker
  auto arc = rusty::Arc<PollThreadWorker>::make();

  // Clone Arc for thread
  auto thread_arc = arc.clone();

  // Spawn thread with explicit parameter passing (enforces Send trait checking)
  // This properly validates that Arc<PollThreadWorker> is Send
  auto handle = rusty::thread::spawn(
    [](rusty::Arc<PollThreadWorker> arc) {
      arc->poll_loop();
    },
    thread_arc
  );

  // Store handle (using const method)
  *arc->join_handle_.borrow_mut() = rusty::Some(std::move(handle));

  return arc;
}

// Explicit shutdown method
void PollThreadWorker::shutdown() const {
  // Remove all pollables before stopping
  for (auto& pair : *fd_to_pollable_.borrow()) {
    // Arc provides const access, but remove() needs non-const reference
    // Safe: we're managing the lifecycle and shutting down
    this->remove(const_cast<Pollable&>(*pair.second));
  }

  // Signal thread to stop
  (**stop_flag_.borrow_mut()).store(true);

  // Join thread
  if (join_handle_.borrow()->is_some()) {
    join_handle_.borrow_mut()->take().unwrap().join();
  }
}

// Destructor just warns if not shut down
PollThreadWorker::~PollThreadWorker() {
  // Check stop flag value
  if (!(**stop_flag_.borrow()).load()) {
    Log_error("PollThreadWorker destroyed without shutdown() - thread may leak!");
  }
}

// @unsafe - Triggers ready jobs in coroutines
// SAFETY: Uses spinlock for thread safety
void PollThreadWorker::TriggerJob() const {
  (**lock_job_.borrow_mut()).lock();
  auto jobs_exec = *set_sp_jobs_.borrow();
  set_sp_jobs_.borrow_mut()->clear();
  (**lock_job_.borrow_mut()).unlock();

  // Process Arc<Job> jobs
  auto it = jobs_exec.begin();
  while (it != jobs_exec.end()) {
    auto sp_job = *it;
    // Arc provides const access, but Job methods need mutable access
    // Safe: we're managing the job lifecycle and calling virtual methods
    Job* job_ptr = const_cast<Job*>(sp_job.get());
    if (job_ptr->Ready()) {
      Coroutine::CreateRun([job_ptr]() {job_ptr->Work();});
      it = jobs_exec.erase(it);
    }
    else {
      it++;
    }
  }
}

// @unsafe - Main polling loop with complex synchronization
// SAFETY: Uses spinlocks and proper synchronization primitives
void PollThreadWorker::poll_loop() const {
  while (!(**stop_flag_.borrow()).load()) {
    TriggerJob();
    // Wait() now directly casts userdata to Pollable* and calls handlers
    // Safe because deferred removal guarantees object stays in fd_to_pollable_ map
    poll_.borrow_mut()->Wait();
    TriggerJob();

    // Process deferred removals AFTER all events handled
    (**pending_remove_l_.borrow_mut()).lock();
    std::unordered_set<int> remove_fds = std::move(*pending_remove_.borrow_mut());
    pending_remove_.borrow_mut()->clear();
    (**pending_remove_l_.borrow_mut()).unlock();

    for (int fd : remove_fds) {
      (**l_.borrow_mut()).lock();

      auto it = fd_to_pollable_.borrow()->find(fd);
      if (it != fd_to_pollable_.borrow()->end()) {
        auto sp_poll = it->second;

        // Check if fd was NOT reused (still in mode_ map)
        if (mode_.borrow()->find(fd) != mode_.borrow()->end()) {
          poll_.borrow_mut()->Remove(sp_poll);
        }

        // Remove from map - object may be destroyed here
        fd_to_pollable_.borrow_mut()->erase(it);
        mode_.borrow_mut()->erase(fd);
      }

      (**l_.borrow_mut()).unlock();
    }
    TriggerJob();
    // Rc gives const access, Loop() is const (safe: thread-local)
    auto reactor_rc = Reactor::GetReactor();
    reactor_rc->Loop();
  }

  // Process any final pending removals after stop_flag_ is set
  // This ensures destructor cleanup is processed even if the thread
  // exits the loop before processing the last batch
  (**pending_remove_l_.borrow_mut()).lock();
  std::unordered_set<int> remove_fds = std::move(*pending_remove_.borrow_mut());
  pending_remove_.borrow_mut()->clear();
  (**pending_remove_l_.borrow_mut()).unlock();

  for (int fd : remove_fds) {
    (**l_.borrow_mut()).lock();

    auto it = fd_to_pollable_.borrow()->find(fd);
    if (it != fd_to_pollable_.borrow()->end()) {
      auto sp_poll = it->second;

      // Check if fd was NOT reused (still in mode_ map)
      if (mode_.borrow()->find(fd) != mode_.borrow()->end()) {
        poll_.borrow_mut()->Remove(sp_poll);
      }

      // Remove from map - object may be destroyed here
      fd_to_pollable_.borrow_mut()->erase(it);
      mode_.borrow_mut()->erase(fd);
    }

    (**l_.borrow_mut()).unlock();
  }
}

// @safe - Thread-safe job addition with polymorphic Arc
void PollThreadWorker::add(rusty::Arc<Job> sp_job) const {
  (**lock_job_.borrow_mut()).lock();
  set_sp_jobs_.borrow_mut()->insert(sp_job);
  (**lock_job_.borrow_mut()).unlock();
}

// @safe - Thread-safe job removal with polymorphic Arc
void PollThreadWorker::remove(rusty::Arc<Job> sp_job) const {
  (**lock_job_.borrow_mut()).lock();
  set_sp_jobs_.borrow_mut()->erase(sp_job);
  (**lock_job_.borrow_mut()).unlock();
}

// @safe - Adds pollable with polymorphic Arc ownership
// SAFETY: Stores Arc in map, passes raw pointer to epoll for fast lookup
void PollThreadWorker::add(rusty::Arc<Pollable> sp_poll) const{
  int fd = sp_poll->fd();
  int poll_mode = sp_poll->poll_mode();

  (**l_.borrow_mut()).lock();

  // Check if already exists
  if (fd_to_pollable_.borrow()->find(fd) != fd_to_pollable_.borrow()->end()) {
    (**l_.borrow_mut()).unlock();
    return;
  }

  // Store in map
  (*fd_to_pollable_.borrow_mut())[fd] = sp_poll.clone();
  (*mode_.borrow_mut())[fd] = poll_mode;

  // userdata = raw Pollable* for lookup (safe - kept alive by fd_to_pollable_ map)
  // Arc::get() returns const pointer, but epoll needs void* userdata
  // Safe: userdata is only used as an opaque identifier, not for mutation
  void* userdata = const_cast<void*>(static_cast<const void*>(sp_poll.get()));

  poll_.borrow_mut()->Add(sp_poll, userdata);

  (**l_.borrow_mut()).unlock();
}

// @unsafe - Removes pollable with deferred cleanup
// SAFETY: Deferred removal ensures safe cleanup
void PollThreadWorker::remove(Pollable& poll) const {
  int fd = poll.fd();

  (**l_.borrow_mut()).lock();
  bool found = (fd_to_pollable_.borrow()->find(fd) != fd_to_pollable_.borrow()->end());
  (**l_.borrow_mut()).unlock();

  if (!found) {
    return;  // Not found
  }

  // Add to pending_remove (actual removal happens after epoll_wait)
  (**pending_remove_l_.borrow_mut()).lock();
  pending_remove_.borrow_mut()->insert(fd);
  (**pending_remove_l_.borrow_mut()).unlock();
}

// @unsafe - Updates poll mode
// SAFETY: Protected by spinlock, validates poll existence
void PollThreadWorker::update_mode(Pollable& poll, int new_mode) const {
  int fd = poll.fd();
  (**l_.borrow_mut()).lock();

  // Verify the pollable is registered
  if (fd_to_pollable_.borrow()->find(fd) == fd_to_pollable_.borrow()->end()) {
    (**l_.borrow_mut()).unlock();
    return;
  }

  auto mode_it = mode_.borrow()->find(fd);
  verify(mode_it != mode_.borrow()->end());
  int old_mode = mode_it->second;
  (*mode_.borrow_mut())[fd] = new_mode;

  if (new_mode != old_mode) {
    void* userdata = &poll;  // Use address of reference
    poll_.borrow_mut()->Update(poll, userdata, new_mode, old_mode);
  }

  (**l_.borrow_mut()).unlock();
}

} // namespace rrr
