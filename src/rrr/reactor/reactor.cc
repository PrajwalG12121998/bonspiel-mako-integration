
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

thread_local std::shared_ptr<Reactor> Reactor::sp_reactor_th_{};
thread_local std::shared_ptr<Coroutine> Reactor::sp_running_coro_th_{};

// @safe - Returns current thread-local coroutine
std::shared_ptr<Coroutine> Coroutine::CurrentCoroutine() {
  // TODO re-enable this verify
//  verify(sp_running_coro_th_);
  return Reactor::sp_running_coro_th_;
}

// @unsafe - Creates and runs a new coroutine with function wrapping
// SAFETY: Reactor manages coroutine lifecycle properly
std::shared_ptr<Coroutine>
Coroutine::CreateRun(std::function<void()> func) {
  auto& reactor = *Reactor::GetReactor();
  auto coro = reactor.CreateRunCoroutine(func);
  // some events might be triggered in the last coroutine.
  return coro;
}

// @unsafe - Returns thread-local reactor instance, creates if needed
// SAFETY: Thread-local storage ensures thread safety
std::shared_ptr<Reactor>
Reactor::GetReactor() {
  if (!sp_reactor_th_) {
    Log_debug("create a coroutine scheduler");
    sp_reactor_th_ = std::make_shared<Reactor>();
    sp_reactor_th_->thread_id_ = std::this_thread::get_id();
  }
  return sp_reactor_th_;
}

/**
 * @param func
 * @return
 */
// @unsafe - Creates and runs coroutine with complex state management
// SAFETY: Proper lifecycle management with shared_ptr
std::shared_ptr<Coroutine>
Reactor::CreateRunCoroutine(const std::function<void()> func) {
  std::shared_ptr<Coroutine> sp_coro;
  if (REUSING_CORO && available_coros_.size() > 0) {
    //Log_info("Reusing stuff");
    sp_coro = available_coros_.back();
    available_coros_.pop_back();
    sp_coro->func_ = func;
  } else {
    sp_coro = std::make_shared<Coroutine>(func);
  }
  
  // Save old coroutine context
  auto sp_old_coro = sp_running_coro_th_;
  sp_running_coro_th_ = sp_coro;
  
  verify(sp_coro);
  auto pair = coros_.insert(sp_coro);
  verify(pair.second);
  verify(coros_.size() > 0);
  
  sp_coro->Run();
  if (sp_coro->Finished()) {
    coros_.erase(sp_coro);
  }
  
  Loop();
  
  // yielded or finished, reset to old coro.
  sp_running_coro_th_ = sp_old_coro;
  return sp_coro;
}

// @safe - Checks timeout events and moves ready ones to ready list
void Reactor::CheckTimeout(std::vector<std::shared_ptr<Event>>& ready_events ) {
  auto time_now = Time::now(true);
  for (auto it = timeout_events_.begin(); it != timeout_events_.end();) {
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
          it = timeout_events_.erase(it);
        } else {
          it++;
        }
        break;
      }
      case Event::READY:
      case Event::DONE:
        it = timeout_events_.erase(it);
        break;
      default:
        verify(0);
    }
  }

}

//  be careful this could be called from different coroutines.
// @unsafe - Main event loop with complex event processing
// SAFETY: Thread-safe via thread_id verification
void Reactor::Loop(bool infinite) {
  verify(std::this_thread::get_id() == thread_id_);
  looping_ = infinite;
  do {
    // Keep processing events until no new ready events are found
    // This fixes the event chain propagation issue
    bool found_ready_events = true;
    while (found_ready_events) {
      found_ready_events = false;
      std::vector<shared_ptr<Event>> ready_events;
      
      // Check waiting events
      auto& events = waiting_events_;
      for (auto it = events.begin(); it != events.end();) {
        Event& event = **it;
        event.Test();
        if (event.status_ == Event::READY) {
          ready_events.push_back(std::move(*it));
          it = events.erase(it);
          found_ready_events = true;
        } else if (event.status_ == Event::DONE) {
          it = events.erase(it);
        } else {
          it ++;
        }
      }
      
      CheckTimeout(ready_events);
      
      // Process ready events
      for (auto& up_ev: ready_events) {
        auto& event = *up_ev;
        auto sp_coro = event.wp_coro_.lock();
        verify(sp_coro);
        verify(coros_.find(sp_coro) != coros_.end());
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
  } while (looping_);
}

// @unsafe - Continues execution of paused coroutine
// SAFETY: Manages coroutine state transitions properly
void Reactor::ContinueCoro(std::shared_ptr<Coroutine> sp_coro) {
//  verify(!sp_running_coro_th_); // disallow nested coros
  auto sp_old_coro = sp_running_coro_th_;
  sp_running_coro_th_ = sp_coro;
  verify(!sp_running_coro_th_->Finished());
  if (sp_coro->status_ == Coroutine::INIT) {
    sp_coro->Run();
  } else {
    // PAUSED or RECYCLED
    sp_running_coro_th_->Continue();
  }
  if (sp_running_coro_th_->Finished()) {
    if (REUSING_CORO) {
      sp_coro->status_ = Coroutine::RECYCLED;
      available_coros_.push_back(sp_running_coro_th_);
    }
    coros_.erase(sp_running_coro_th_);
  }
  sp_running_coro_th_ = sp_old_coro;
}

// TODO PollThread -> Reactor
// TODO PollMgr -> ReactorFactory
class PollMgr::PollThread {

  friend class PollMgr;

  Epoll poll_{};

  SpinLock l_;
  // Authoritative storage: fd -> shared_ptr<Pollable>
  std::unordered_map<int, std::shared_ptr<Pollable>> fd_to_pollable_;
  std::unordered_map<int, int> mode_; // fd->mode

  std::set<std::shared_ptr<Job>> set_sp_jobs_;

  std::unordered_set<int> pending_remove_;  // Store fds to remove
  SpinLock pending_remove_l_;
  SpinLock lock_job_;

  pthread_t th_;
  bool stop_flag_;

  // @unsafe - C-style thread entry point with raw pointer cast
  // SAFETY: arg is always valid PollThread* from start()
  static void* start_poll_loop(void* arg) {
    PollThread* thiz = (PollThread*) arg;
    thiz->poll_loop();
    pthread_exit(nullptr);
    return nullptr;
  }

  void poll_loop();

  // @unsafe - Creates pthread with raw pointer passing
  // SAFETY: 'this' remains valid throughout thread lifetime
  void start(PollMgr* poll_mgr) {
    Pthread_create(&th_, nullptr, PollMgr::PollThread::start_poll_loop, this);
    pthread_setname_np(th_, "Follower server thread"); 
  }

  // @unsafe - Triggers ready jobs in coroutines
  // SAFETY: Uses spinlock for thread safety
  void TriggerJob() {
    lock_job_.lock();
    auto jobs_exec = set_sp_jobs_;
    set_sp_jobs_.clear();
    lock_job_.unlock();
    auto it = jobs_exec.begin();
    while (it != jobs_exec.end()) {
      auto sp_job = *it;
      if (sp_job->Ready()) {
        Coroutine::CreateRun([sp_job]() {sp_job->Work();});
        it = jobs_exec.erase(it);
      }
      else {
        it++;
      }
    }
  }

 public:

  PollThread() : stop_flag_(false) {
  }

  ~PollThread() {
    stop_flag_ = true;
    Pthread_join(th_, nullptr);

    // when stopping, remove anything registered in pollmgr
    for (auto& pair : fd_to_pollable_) {
      this->remove(pair.second);
    }
    // shared_ptrs automatically cleaned up
  }

  void add(std::shared_ptr<Pollable> poll);
  void remove(std::shared_ptr<Pollable> poll);
  void update_mode(std::shared_ptr<Pollable> poll, int new_mode);

  void add(std::shared_ptr<Job>);
  void remove(std::shared_ptr<Job>);
};

// @unsafe - Allocates raw array and creates threads
// SAFETY: Array properly deleted in destructor; threads joined before deletion
PollMgr::PollMgr(int n_threads /* =... */)
    : n_threads_(n_threads), poll_threads_() {
  verify(n_threads_ > 0);
  poll_threads_.reserve(n_threads_);
  for (int i = 0; i < n_threads_; i++) {
    poll_threads_.push(rusty::make_box<PollThread>());
    poll_threads_[i]->start(this);
  }
}

// @unsafe - Returns raw pointer to pthread handle
// SAFETY: Valid as long as PollMgr exists and i < n_threads_
pthread_t* PollMgr::GetPthreads(int i) {
  return &poll_threads_[i]->th_;
}

// @safe - Vec automatically cleans up
PollMgr::~PollMgr() {
  // Vec destructor handles cleanup automatically
  //Log_debug("rrr::PollMgr: destroyed");
}

// @unsafe - Main polling loop with complex synchronization
// SAFETY: Uses spinlocks and proper synchronization primitives
void PollMgr::PollThread::poll_loop() {
  while (!stop_flag_) {
    TriggerJob();
    // Pass lookup lambda: userdata is Pollable*, lookup shared_ptr by fd
    poll_.Wait([this](void* userdata) -> std::shared_ptr<Pollable> {
        Pollable* poll_ptr = reinterpret_cast<Pollable*>(userdata);
        int fd = poll_ptr->fd();  // Safe - object still in map

        l_.lock();
        auto it = fd_to_pollable_.find(fd);
        std::shared_ptr<Pollable> result;
        if (it != fd_to_pollable_.end()) {
          result = it->second;
        }
        l_.unlock();

        return result;
    });
    TriggerJob();

    // Process deferred removals AFTER all events handled
    pending_remove_l_.lock();
    std::unordered_set<int> remove_fds = std::move(pending_remove_);
    pending_remove_.clear();
    pending_remove_l_.unlock();

    for (int fd : remove_fds) {
      l_.lock();

      auto it = fd_to_pollable_.find(fd);
      if (it == fd_to_pollable_.end()) {
        l_.unlock();
        continue;
      }

      auto sp_poll = it->second;

      // Check if fd was reused
      if (mode_.find(fd) == mode_.end()) {
        poll_.Remove(sp_poll);
      }

      // Remove from map - object may be destroyed here
      fd_to_pollable_.erase(it);
      mode_.erase(fd);

      l_.unlock();
    }
    TriggerJob();
    Reactor::GetReactor()->Loop();
  }
}

// @safe - Thread-safe job addition with spinlock
void PollMgr::PollThread::add(std::shared_ptr<Job> sp_job) {
  lock_job_.lock();
  set_sp_jobs_.insert(sp_job);
  lock_job_.unlock();
}

// @safe - Thread-safe job removal with spinlock
void PollMgr::PollThread::remove(std::shared_ptr<Job> sp_job) {
  lock_job_.lock();
  set_sp_jobs_.erase(sp_job);
  lock_job_.unlock();
}

// @safe - Adds pollable with shared_ptr ownership
// SAFETY: Stores shared_ptr in map, passes raw pointer to epoll
void PollMgr::PollThread::add(std::shared_ptr<Pollable> sp_poll) {
  int fd = sp_poll->fd();
  int poll_mode = sp_poll->poll_mode();

  l_.lock();

  // Check if already exists
  if (fd_to_pollable_.find(fd) != fd_to_pollable_.end()) {
    l_.unlock();
    return;
  }

  // Store in map
  fd_to_pollable_[fd] = sp_poll;
  mode_[fd] = poll_mode;

  // userdata = raw Pollable* for lookup
  void* userdata = sp_poll.get();

  poll_.Add(sp_poll, userdata);

  l_.unlock();
}

// @unsafe - Removes pollable with deferred cleanup
// SAFETY: Deferred removal ensures safe cleanup
void PollMgr::PollThread::remove(std::shared_ptr<Pollable> sp_poll) {
  int fd = sp_poll->fd();

  l_.lock();
  if (fd_to_pollable_.find(fd) == fd_to_pollable_.end()) {
    l_.unlock();
    return;  // Not found
  }
  l_.unlock();

  // Add to pending_remove (actual removal happens after epoll_wait)
  pending_remove_l_.lock();
  pending_remove_.insert(fd);
  pending_remove_l_.unlock();
}

// @unsafe - Updates poll mode
// SAFETY: Protected by spinlock, validates poll existence
void PollMgr::PollThread::update_mode(std::shared_ptr<Pollable> sp_poll, int new_mode) {
  int fd = sp_poll->fd();

  l_.lock();

  if (fd_to_pollable_.find(fd) == fd_to_pollable_.end()) {
    l_.unlock();
    return;
  }

  auto mode_it = mode_.find(fd);
  verify(mode_it != mode_.end());
  int old_mode = mode_it->second;
  mode_it->second = new_mode;

  if (new_mode != old_mode) {
    void* userdata = sp_poll.get();  // Raw pointer
    poll_.Update(sp_poll, userdata, new_mode, old_mode);
  }

  l_.unlock();
}

// @safe - Pure hash function with no side effects
static inline uint32_t hash_fd(uint32_t key) {
  uint32_t c2 = 0x27d4eb2d; // a prime or an odd constant
  key = (key ^ 61) ^ (key >> 16);
  key = key + (key << 3);
  key = key ^ (key >> 4);
  key = key * c2;
  key = key ^ (key >> 15);
  return key;
}

// @safe - Routes pollable to thread based on fd hash
// SAFETY: Hash ensures consistent thread assignment
void PollMgr::add(std::shared_ptr<Pollable> poll) {
  int fd = poll->fd();
  if (fd >= 0) {
    int tid = hash_fd(fd) % n_threads_;
    poll_threads_[tid]->add(poll);
  }
}

// @safe - Routes removal to correct thread
// SAFETY: Uses same hash as add() for consistency
void PollMgr::remove(std::shared_ptr<Pollable> poll) {
  int fd = poll->fd();
  if (fd >= 0) {
    int tid = hash_fd(fd) % n_threads_;
    poll_threads_[tid]->remove(poll);
  }
}

// @safe - Routes mode update to correct thread
// SAFETY: Uses same hash as add() for consistency
void PollMgr::update_mode(std::shared_ptr<Pollable> poll, int new_mode) {
  int fd = poll->fd();
  if (fd >= 0) {
    int tid = hash_fd(fd) % n_threads_;
    poll_threads_[tid]->update_mode(poll, new_mode);
  }
}

// @safe - Adds job to first poll thread
void PollMgr::add(std::shared_ptr<Job> fjob) {
  int tid = 0;
  poll_threads_[tid]->add(fjob);
}

// @safe - Removes job from first poll thread
void PollMgr::remove(std::shared_ptr<Job> fjob) {
  int tid = 0;
  poll_threads_[tid]->remove(fjob);
}

} // namespace rrr
