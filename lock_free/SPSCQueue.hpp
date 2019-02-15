#pragma once

#include <atomic>
#include <cassert>
#include <cstdlib>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace lockfree
{

template<typename T>
struct SPSCQueue {
  using value_type = T;

  SPSCQueue(const SPSCQueue&) = delete;
  SPSCQueue& operator=(const SPSCQueue&) = delete;

  explicit SPSCQueue(size_t capacity):
    capacity_(capacity),
    records_(static_cast<T*>(std::malloc(sizeof(T) * capacity_))),
    read_cursor_(0),
    write_cursor_(0) {
    assert(capacity >= 2);
    if (!records_) {
      throw std::bad_alloc();
    }
  }

  ~SPSCQueue() {
    if constexpr (!std::is_trivially_destructible<T>::value) {
      size_t read_ticket = read_cursor_;
      size_t write_ticket = write_cursor_;
      while (read_ticket != write_ticket) {
        records_[read_ticket].~T();
        if (++read_ticket == capacity_) {
          read_ticket = 0;
        }
      }
    }
    std::free(records_);
  }

  template<typename ...Args>
  bool try_emplace(Args&&... args) {
    auto const write_ticket = write_cursor_.load(std::memory_order_relaxed);
    auto next_write = (write_ticket + 1);
    if (next_write == capacity_) {
      next_write = 0;
    }

    if (next_write != read_cursor_.load(std::memory_order_acquire)) {
      new (&records_[next_write]) T(std::forward<Args>(args)...);
      write_cursor_.store(next_write, std::memory_order_release);
      return true;
    }
    return false;
  }

  bool try_pop(T& v) {
    auto const read_ticket = read_cursor_.load(std::memory_order_relaxed);
    if (read_ticket == write_cursor_.load(std::memory_order_acquire)) {
      return false;
    }
    auto next_read = (read_ticket + 1);
    if (next_read == capacity_) {
      next_read = 0;
    }

    v = std::move(records_[read_ticket]);
    records_[read_ticket].~T();
    read_cursor_.store(next_read, std::memory_order_release);
    return true;
  }

  T* front_ptr() {
    auto const read_ticket = read_cursor_.load(std::memory_order_relaxed);
    if (read_ticket == write_cursor_.load(std::memory_order_acquire)) {
      return nullptr;
    }
    return records_[read_ticket];
  }

  void pop_front() {
    auto const read_ticket = read_cursor_.load(std::memory_order_relaxed);
    assert(read_ticket != write_cursor_.load(std::memory_order_acquire));
    auto next_read = read_ticket + 1;
    if (next_read == capacity_) {
      next_read = 0;
    }
    records_[read_ticket].~T();
    read_cursor_.store(next_read, std::memory_order_release);
  }

  bool empty() const {
    return read_cursor_.load(std::memory_order_acquire)
      == write_cursor_.load(std::memory_order_acquire);
  }

  bool full() const {
    auto next_write = write_cursor_.load(std::memory_order_acquire) + 1;
    if (next_write == capacity_) {
      next_write = 0;
    }
    if (next_write == read_cursor_.load(std::memory_order_acquire)) {
      return false;
    }
    return true;
  }

  size_t guess_size() const {
    int ret = write_cursor_.load(std::memory_order_acquire) -
      read_cursor_.load(std::memory_order_acquire);
    if (ret < 0) {
      ret += capacity_;
    }
    return ret;
  }

  size_t capacity() const {
    return capacity_ - 1;
  }

private:
  char pad0_[hardware_destructive_interference_size];
  const size_t capacity_;
  T* const records_;
  alignas(hardware_destructive_interference_size) std::atomic<size_t> read_cursor_;
  alignas(hardware_destructive_interference_size) std::atomic<size_t> write_cursor_;
  char pad1_[hardware_destructive_interference_size - sizeof(std::atomic<size_t>)];

};
} /* lockfree */
