#include <utility>

#include <functional>
#include <iostream>
#include "../base/all.hpp"
#include "coroutine.h"
#include "reactor.h"

namespace rrr {

Coroutine::Coroutine(std::move_only_function<void()> func)
    : status_(INIT),
      func_(std::move(func)),
      boost_coro_task_(rusty::None),
      boost_coro_yield_(boost::none) {
}

Coroutine::~Coroutine() {
  // rusty::Box automatically handles cleanup
//  verify(0);
}

void Coroutine::BoostRunWrapper(boost_coro_yield_t& yield) {
  *boost_coro_yield_.borrow_mut() = yield;
  verify(*func_.borrow());
  auto reactor = Reactor::GetReactor();
//  reactor->coros_;
  while (true) {
    auto sz = reactor->coros_.borrow()->size();
    verify(sz > 0);
    (*func_.borrow_mut())();
    *func_.borrow_mut() = {};
    status_.set(FINISHED);
    yield();
  }
}

void Coroutine::Run() const {
  verify(boost_coro_task_.borrow()->is_none());
  verify(status_.get() == INIT);
  status_.set(STARTED);
  auto reactor = Reactor::GetReactor();
//  reactor->coros_;
  auto sz = reactor->coros_.borrow()->size();
  verify(sz > 0);
  auto task = std::bind(&Coroutine::BoostRunWrapper, const_cast<Coroutine*>(this), std::placeholders::_1);
  *boost_coro_task_.borrow_mut() = rusty::Some(rusty::make_box<boost_coro_task_t>(std::move(task)));
#ifdef USE_BOOST_COROUTINE1
  (*boost_coro_task_.borrow()->unwrap())();
#endif
}

void Coroutine::Yield() const {
  verify(*boost_coro_yield_.borrow());
  verify(status_.get() == STARTED || status_.get() == RESUMED);
  status_.set(PAUSED);
  boost_coro_yield_.borrow()->value()();
}

void Coroutine::Continue() const {
  verify(status_.get() == PAUSED || status_.get() == RECYCLED);
  verify(boost_coro_task_.borrow()->is_some());
  status_.set(RESUMED);
  (*boost_coro_task_.borrow_mut()->unwrap())();
  // some events might have been triggered from last coroutine,
  // but you have to manually call the scheduler to loop.
}

bool Coroutine::Finished() const {
  return status_.get() == FINISHED || status_.get() == RECYCLED;
}

} // namespace rrr
