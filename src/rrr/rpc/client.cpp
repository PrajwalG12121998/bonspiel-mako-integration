#include <string>
#include <memory>
#include <chrono>
#include <mutex>
#include <condition_variable>

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netdb.h>
#include <netinet/tcp.h>

#include "reactor/coroutine.h"
#include "client.hpp"
#include "utils.hpp"

using namespace std;

// External safety annotations for std functions used in this module
// @external: {
//   std::unique_lock::unique_lock: [unsafe, (std::mutex&) -> std::unique_lock<std::mutex>]
//   std::lock_guard::lock_guard: [unsafe, (std::mutex&) -> std::lock_guard<std::mutex>]
//   std::condition_variable::wait: [unsafe, (std::unique_lock<std::mutex>&, auto) -> void]
// }

namespace rrr {

// @unsafe - Calls std::unique_lock and std::condition_variable::wait (external unsafe)
// SAFETY: Thread-safe RAII lock and wait operations
void Future::wait() {
  std::unique_lock<std::mutex> lock(ready_m_);
  ready_cond_.wait(lock, [this]() { return ready_ || timed_out_; });
}

// @unsafe - Calls std::unique_lock (external unsafe)
// SAFETY: Thread-safe RAII lock and wait_for operations
void Future::timed_wait(double sec) {
  std::unique_lock<std::mutex> lock(ready_m_);

  auto duration = std::chrono::duration<double>(sec);
  bool success = ready_cond_.wait_for(lock, duration, [this]() {
    return ready_ || timed_out_;
  });

  if (!success && !ready_) {
    timed_out_ = true;
    error_code_ = ETIMEDOUT;
  }

  // Release lock before calling callback
  lock.unlock();

  if (timed_out_ && attr_.callback != nullptr) {
    attr_.callback(this);
  }
}

// @unsafe - Calls std::lock_guard (external unsafe)
// SAFETY: Thread-safe RAII lock and notify operations
void Future::notify_ready() {
  bool should_callback = false;
  {
    std::lock_guard<std::mutex> lock(ready_m_);
    if (!timed_out_) {
      ready_ = true;
    }
    should_callback = ready_;
    // Use notify_all to wake up ALL waiting threads
    ready_cond_.notify_all();
  }
  // Execute callback outside lock to avoid deadlock
  if (should_callback && attr_.callback != nullptr) {
    // Warning: make sure memory is safe!
    auto x = attr_.callback;
    Coroutine::CreateRun([x, this]() {
      x(this);
    });
  }
}

// @unsafe - Cancels all pending futures with error
// SAFETY: Protected by spinlock, proper refcount management
void Client::invalidate_pending_futures() {
  list<Future*> futures;
  pending_fu_l_.lock();
  for (auto& it: pending_fu_) {
    futures.push_back(it.second);
  }
  pending_fu_.clear();
  pending_fu_l_.unlock();

  for (auto& fu: futures) {
    if (fu != nullptr) {
      fu->error_code_ = ENOTCONN;
      fu->notify_ready();

      // since we removed it from pending_fu_
      fu->release();
    }
  }
}

// @unsafe - Closes socket and invalidates futures
// SAFETY: Idempotent, proper cleanup sequence
void Client::close() {
  if (status_ == CONNECTED) {
    poll_thread_worker_->remove(*this);
    ::close(sock_);
  }
  status_ = CLOSED;
  invalidate_pending_futures();
}

// @unsafe - Establishes TCP/IPC connection to server
// SAFETY: Proper socket creation, configuration, and error handling
int Client::connect(const char* addr) {
  verify(status_ != CONNECTED);
  string addr_str(addr);
  size_t idx = addr_str.find(":");
  if (idx == string::npos) {
    Log_error("rrr::Client: bad connect address: %s", addr);
    return EINVAL;
  }
  string host = addr_str.substr(0, idx);
  string port = addr_str.substr(idx + 1);
#ifdef USE_IPC
  struct sockaddr_un saun;
  saun.sun_family = AF_UNIX;
  string ipc_addr = "rsock" + port;
  strcpy(saun.sun_path, ipc_addr.data());
  if ((sock_ = socket(AF_UNIX, SOCK_STREAM, 0)) < 0) {
    perror("client: socket");
    exit(1);
  }
  auto len = sizeof(saun.sun_family) + strlen(saun.sun_path)+1;
  if (::connect(sock_, (struct sockaddr*)&saun, len) < 0) {
    perror("client: connect");
    exit(1);
  }
#else

  struct addrinfo hints, * result, * rp;
  memset(&hints, 0, sizeof(struct addrinfo));

  hints.ai_family = AF_INET; // ipv4
  hints.ai_socktype = SOCK_STREAM; // tcp

  int r = getaddrinfo(host.c_str(), port.c_str(), &hints, &result);
  if (r != 0) {
    Log_error("rrr::Client: getaddrinfo(): %s", gai_strerror(r));
    return EINVAL;
  }

  for (rp = result; rp != nullptr; rp = rp->ai_next) {
    sock_ = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
    if (sock_ == -1) {
      continue;
    }

    const int yes = 1;
    verify(setsockopt(sock_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) == 0);
    verify(setsockopt(sock_, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes)) == 0);
    int buf_len = 1024 * 1024;
    setsockopt(sock_, SOL_SOCKET, SO_RCVBUF, &buf_len, sizeof(buf_len));
    setsockopt(sock_, SOL_SOCKET, SO_SNDBUF, &buf_len, sizeof(buf_len));

    if (::connect(sock_, rp->ai_addr, rp->ai_addrlen) == 0) {
      break;
    }
    ::close(sock_);
    sock_ = -1;
  }
  freeaddrinfo(result);

  if (rp == nullptr) {
    // failed to connect
    Log_error("rrr::Client: connect(%s): %s", addr, strerror(errno));
    return ENOTCONN;
  }
#endif
  verify(set_nonblocking(sock_, true) == 0);
  Log_debug("rrr::Client: connected to %s", addr);

  status_ = CONNECTED;
  poll_thread_worker_->add(shared_from_this());

  return 0;
}

// @safe - Simple error handler
void Client::handle_error() {
  close();
}

// @unsafe - Writes buffered data to socket
// SAFETY: Protected by spinlock, handles partial writes
void Client::handle_write() {
  if (status_ != CONNECTED) {
    return;
  }

  out_l_.lock();
  out_.write_to_fd(sock_);
  if (out_.empty()) {
    //Log_info("Client handle_write setting read mode here...");
    poll_thread_worker_->update_mode(*this, Pollable::READ);
  }
  out_l_.unlock();
}

// @unsafe - Reads and processes RPC responses
// SAFETY: Protected by spinlock, validates packet structure
void Client::handle_read() {
  if (status_ != CONNECTED) {
    return;
  }

  int bytes_read = in_.read_from_fd(sock_);
  if (bytes_read == 0) {
    return;
  }

  for (;;) {
    //Log_info("stuck in client handle_read loop");
    i32 packet_size;
    int n_peek = in_.peek(&packet_size, sizeof(i32));
    if (n_peek == sizeof(i32)
        && in_.content_size() >= packet_size + sizeof(i32)) {
      // consume the packet size
      verify(in_.read(&packet_size, sizeof(i32)) == sizeof(i32));

      v64 v_reply_xid;
      v32 v_error_code;

      in_ >> v_reply_xid >> v_error_code;

      pending_fu_l_.lock();
      unordered_map<i64, Future*>::iterator
          it = pending_fu_.find(v_reply_xid.get());
      if (it != pending_fu_.end()) {
        Future* fu = it->second;
        verify(fu->xid_ == v_reply_xid.get());
        pending_fu_.erase(it);
        pending_fu_l_.unlock();

        fu->error_code_ = v_error_code.get();
        fu->reply_.read_from_marshal(in_,
                                     packet_size - v_reply_xid.val_size()
                                         - v_error_code.val_size());

        fu->notify_ready();

        // since we removed it from pending_fu_
        fu->release();
      } else {
        // the future might timed out
        pending_fu_l_.unlock();
      }

    } else {
      // packet incomplete or no more packets to process
      break;
    }
  }
}

// @safe - Determines polling mode based on output buffer
int Client::poll_mode() {
  int mode = Pollable::READ;
  out_l_.lock();
  if (!out_.empty()) {
    mode |= Pollable::WRITE;
  }
  out_l_.unlock();
  return mode;
}

// @unsafe - Starts new RPC request with marshaling
// SAFETY: Protected by spinlocks, proper refcounting
Future* Client::begin_request(i32 rpc_id, const FutureAttr& attr /* =... */) {
  out_l_.lock();

  if (status_ != CONNECTED) {
    return nullptr;
  }

  Future* fu = new Future(xid_counter_.next(), attr);
  pending_fu_l_.lock();
  pending_fu_[fu->xid_] = fu;
  pending_fu_l_.unlock();
  //Log_info("Starting a new request with rpc_id %ld,xid_:%llu", rpc_id,fu->xid_); 
  // check if the client gets closed in the meantime
  if (status_ != CONNECTED) {
    pending_fu_l_.lock();
    unordered_map<i64, Future*>::iterator it = pending_fu_.find(fu->xid_);
    if (it != pending_fu_.end()) {
      it->second->release();
      pending_fu_.erase(it);
    }
    pending_fu_l_.unlock();

    return nullptr;
  }

  bmark_ = rusty::Some(rusty::Box<Marshal::bookmark>(out_.set_bookmark(sizeof(i32)))); // will fill packet size later

  *this << v64(fu->xid_);
  *this << rpc_id;

  // one ref is already in pending_fu_
  return (Future*) fu->ref_copy();
}

// @unsafe - Finalizes request packet with size header
// SAFETY: Updates bookmark, enables write polling
void Client::end_request() {
  // set reply size in packet
  if (bmark_.is_some()) {
    i32 request_size = out_.get_and_reset_write_cnt();
    //Log_info("client request size is %d", request_size);
    out_.write_bookmark(bmark_.unwrap_ref().get(), &request_size);
    bmark_ = rusty::None;  // Reset to None (automatically deletes old value)
  }

  // always enable write events since the code above gauranteed there
  // will be some data to send
  //Log_info("Client end_request setting write mode here....");
  poll_thread_worker_->update_mode(*this, Pollable::READ | Pollable::WRITE);

  out_l_.unlock();
}

// @unsafe - Constructs pool with PollThreadWorker ownership
// SAFETY: Shared ownership of PollThreadWorker
ClientPool::ClientPool(rusty::Arc<PollThreadWorker> poll_thread_worker /* =? */,
                       int parallel_connections /* =? */)
    : parallel_connections_(parallel_connections) {

  verify(parallel_connections_ > 0);
  if (!poll_thread_worker) {
    poll_thread_worker_ = PollThreadWorker::create();
  } else {
    poll_thread_worker_ = poll_thread_worker;
  }
}

// @unsafe - Destroys pool and all cached connections
// SAFETY: Closes all clients and releases PollThreadWorker
ClientPool::~ClientPool() {
  for (auto& it : cache_) {
    for (auto& client : it.second) {
      client->close();
    }
  }

  // Shutdown PollThreadWorker if we own it
  if (poll_thread_worker_) {
    poll_thread_worker_->shutdown();
  }
}

// @unsafe - Gets cached or creates new client connections
// SAFETY: Protected by spinlock, handles connection failures gracefully
std::shared_ptr<Client> ClientPool::get_client(const string& addr) {
  std::shared_ptr<Client> sp_cl = nullptr;
  l_.lock();
  auto it = cache_.find(addr);
  if (it != cache_.end()) {
    sp_cl = it->second[rand_() % parallel_connections_];
  } else {
    std::vector<std::shared_ptr<Client>> parallel_clients;
    bool ok = true;
    for (int i = 0; i < parallel_connections_; i++) {
      auto client = std::make_shared<Client>(this->poll_thread_worker_);
      if (client->connect(addr.c_str()) != 0) {
        ok = false;
        break;
      }
      parallel_clients.push_back(client);
    }
    if (ok) {
      sp_cl = parallel_clients[rand_() % parallel_connections_];
      cache_[addr] = std::move(parallel_clients);
    }
    // If not ok, parallel_clients automatically cleaned up by shared_ptr
  }
  l_.unlock();
  return sp_cl;
}

} // namespace rrr
