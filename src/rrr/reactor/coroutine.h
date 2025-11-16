#pragma once

#include <rusty/box.hpp>
#include <rusty/rc.hpp>
#include <rusty/option.hpp>
#include <rusty/cell.hpp>
#include <rusty/refcell.hpp>

#define USE_BOOST_COROUTINE2

#ifdef USE_BOOST_COROUTINE2
#define BOOST_COROUTINE_NO_DEPRECATION_WARNING 1
#define BOOST_COROUTINES_NO_DEPRECATION_WARNING 1
#include <boost/coroutine2/all.hpp>
#endif

#ifdef USE_BOOST_COROUTINE1
#include <boost/coroutine/symmetric_coroutine.hpp>
#endif

#include <boost/optional.hpp>

//#include <experimental/coroutine>

namespace rrr {

// Forward declaration
class Coroutine;

#ifdef USE_BOOST_COROUTINE2
typedef boost::coroutines2::coroutine<void>::pull_type boost_coro_task_t;
typedef boost::coroutines2::coroutine<void>::push_type boost_coro_yield_t;
typedef boost::coroutines2::coroutine<void()> coro_t;
#endif

#ifdef USE_BOOST_COROUTINE1
    typedef boost::coroutines::symmetric_coroutine<void>::call_type boost_coro_task_t;
    typedef boost::coroutines::symmetric_coroutine<void>::yield_type boost_coro_yield_t;
    typedef boost::coroutines::symmetric_coroutine<void()> coro_t;
#endif

class Reactor;
// @unsafe - Interior mutability with RefCell for const method mutations
class Coroutine {
 public:
  // @safe - Returns current coroutine with single-threaded reference counting
  static rusty::Rc<Coroutine> CurrentCoroutine();
  // the argument cannot be a reference because it could be declared on stack.
  // Using std::move_only_function to support move-only callables (e.g., lambdas capturing rusty::Box)
  // @safe - Creates and runs coroutine with rusty::Rc ownership
  static rusty::Rc<Coroutine> CreateRun(std::move_only_function<void()> func);

  enum Status {INIT=0, STARTED, PAUSED, RESUMED, FINISHED, RECYCLED};

  // Interior mutability for use with rusty::Rc (const methods need to modify state)
  rusty::Cell<Status> status_ = rusty::Cell<Status>(INIT);
  rusty::RefCell<std::move_only_function<void()>> func_;

  // Migrated from std::unique_ptr to rusty::Box with Option for nullable semantics
  rusty::RefCell<rusty::Option<rusty::Box<boost_coro_task_t>>> boost_coro_task_;
  rusty::RefCell<boost::optional<boost_coro_yield_t&>> boost_coro_yield_;

  Coroutine() = delete;
  Coroutine(std::move_only_function<void()> func);
  ~Coroutine();
  void BoostRunWrapper(boost_coro_yield_t& yield);
  void Run() const;  // Made const for Rc compatibility
  void Yield() const;  // Made const for Rc compatibility
  void Continue() const;  // Made const for Rc compatibility
  bool Finished() const;

  // Comparison operator for std::set<rusty::Rc<Coroutine>>
  // Compares by address (pointer identity)
  friend bool operator<(const rusty::Rc<Coroutine>& lhs, const rusty::Rc<Coroutine>& rhs) {
    return lhs.get() < rhs.get();
  }
};

} // namespace rrr
