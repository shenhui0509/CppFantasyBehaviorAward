#pragma once

#include <atomic>
#include <array>
#include <cassert>
#include <cstdint>
#include <mutex>
#include <type_traits>

namespace lock_free
{

namespace detail
{

class Sleeper {
  static constexpr uint32_t kMaxActiveSpin = 4000;
  uint32_t spin_count_;

 public:
  Sleeper() : spin_count_(0) {
  }

  void wait() {
    if (spin_count_ < kMaxActiveSpin) {
      ++spin_count_;
      asm volatile("pause");
    } else {
      struct timespec ts = {0, 500000};
      nanosleep(&ts, nullptr);
    }
  }
};

} /* detail */

struct MicroSpinLock {
  enum { FREE = 0, LOCKED = 1 };
  uint8_t lock_;

  void init() {
    payload()->store(FREE);
  }

  bool try_lock() {
    return cas(FREE, LOCKED);
  }

  void lock() {
    detail::Sleeper sleeper;
    while (!try_lock()) {
      do {
        sleeper.wait();
      } while (payload()->load(std::memory_order_relaxed) == LOCKED);
    }
    assert(payload()->load() == LOCKED);
  }

  void unlock() {
    assert(payload()->load() == LOCKED);
    payload()->store(FREE, std::memory_order_release);
  }

 private:
  std::atomic<uint8_t>* payload() {
    return reinterpret_cast<std::atomic<uint8_t>*>(&this->lock_);
  }

  bool cas(uint8_t compare, uint8_t new_val) {
    return std::atomic_compare_exchange_strong_explicit(
        payload(),
        &compare, new_val,
        std::memory_order_acquire,
        std::memory_order_relaxed);
  }
};

static_assert(std::is_pod<MicroSpinLock>::value,
              "MicroSpinLock must be POD type");
} /*  lock_free */

