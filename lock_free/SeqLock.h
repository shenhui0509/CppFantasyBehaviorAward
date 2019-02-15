#pragma once

#include <atomic>
#include <type_traits>

#ifndef NDEBUG
#define LOCK_FREE_NOINLINE __attribute__((noinline))
#else
#define LOCK_FREE_NOINLINE
#endif

namespace lock_free
{

template<typename T>
class SeqLock {
 public:
  static_assert(std::is_nothrow_copy_assignable<T>::value, "T must be nothrow copy assignable");
  static_assert(std::is_trivially_copy_assignable<T>::value, "T must be trivially copy assignable");

  SeqLock() : seq_(0) {}

  LOCK_FREE_NOINLINE T load() const noexcept {
    T copy;
    std::size_t seq0, seq1;
    do {
      seq0 = seq_.load(std::memory_order_acquire);
      std::atomic_signal_fence(std::memory_order_acq_rel);
      copy = value_;
      std::atomic_signal_fence(std::memory_order_acq_rel);
      seq1 = seq_.load(std::memory_order_acquire);
    } while (seq0 != seq1 || seq0 & 1);
    return copy;
  }

  LOCK_FREE_NOINLINE void store(const T& val) noexcept {
    std::size_t seq0 = seq_.load(std::memory_order_acquire);
    seq_.store(seq0 + 1, std::memory_order_release);
    std::atomic_signal_fence(std::memory_order_acq_rel);
    value_ = val;
    std::atomic_signal_fence(std::memory_order_acq_rel);
    seq_store(seq0 + 2, std::memory_order_release);
  }

 private:
  static constexpr size_t kCachelineSize = hardware_destructive_interference_size;
  alignas(kCachelineSize) T value_;
  std::atomic<std::size_t> seq_;
  char pad_[kCachelineSize -
            ((sizeof(value_) + sizeof(seq_)) % kCachelineSize)];
  static_assert(
      ((sizeof(value_) + sizeof(seq_) + sizeof(pad_))
       % kCachelineSize)
      == 0,
      "sizeof(SeqLock<T>) must be a multiple of CacheLineSize");
};

}
