#pragma once

#include <atomic>
#include <cassert>
#include <limits>
#include <memory>
#include <stdexcept>

namespace lock_free {

static constexpr size_t kCachelineSize = 64;

/*
 *template<typename T,
 *    typename = typename std::enable_if<
 *        std::is_integral<T>::value
 *     && (sizeof(T) <= sizeof(unsigned long long int))
 *     && !std::is_same<T, bool>::value
 *    >::type>
 *unsigned int find_last_set(T& const x) {
 *    using U0 = unsigned int;
 *    using U1 = unsigned long int;
 *    using U2 = unsigned long long int;
 *
 *}
 *
 *template<typename T,
 *    typename = typename std::enable_if<
 *        std::is_unsigned<T>::value>::type>
 *T nextPow2(const T& x) {
 *    return x ? ( T(1) << find_last_set(x - 1)) : T(1);
 *}
 */

template <typename T>
class MPMCQueue {
  static_assert(std::is_nothrow_copy_assignable<T>::value ||
                    std::is_nothrow_move_assignable<T>::value,
                "T must be nothrow copy or move assignable");
  static_assert(std::is_nothrow_destructible<T>::value,
                "T must be nothrow destructible");

 public:
  explicit MPMCQueue(const size_t& capacity)
      : capacity_(capacity), write_cursor_(0), read_cursor_(0) {
    if (capacity_ < 1) {
      throw std::invalid_argument("capacity < 1");
    }
    size_t space = capacity_ * sizeof(Slot) + kCachelineSize - 1;
    buf_ = ::malloc(space);
    if (buf_ == nullptr) {
      throw std::bad_alloc();
    }
    void* buf = buf_;
    slots_ = reinterpret_cast<Slot*>(
        std::align(kCachelineSize, capacity_ * sizeof(Slot), buf, space));
    if (slots_ == nullptr) {
      free(buf_);
      throw std::bad_alloc();
    }

    Slot* p = slots_;
    for (size_t i = 0; i < capacity_; ++i, ++p) {
      new (p) Slot();
    }

    static_assert(sizeof(MPMCQueue<T>) % kCachelineSize == 0,
                  "MPMCQueue<T> size must be a multiple of"
                  "cache line size to prevent false sharing"
                  "between adjacent queues");
    static_assert(sizeof(Slot) % kCachelineSize == 0,
                  "Slot size must be a multiple of"
                  "cache line size to prevent false sharing"
                  "between adjacent slots");
    assert(reinterpret_cast<intptr_t>(slots_) % kCachelineSize == 0 &&
           "slots_ array must be aligned to cache line size"
           "to prevent false sharing between adjacent slots");
    assert(reinterpret_cast<intptr_t>(&write_cursor_) -
                   reinterpret_cast<intptr_t>(&read_cursor_) >=
               kCachelineSize &&
           "write_cursor and read cursor must be a cache line apart"
           "to prevent false sharing");
  }

  ~MPMCQueue() noexcept {
    Slot* p = slots_;
    for (size_t i = 0; i < capacity_; ++i, ++p) {
      p->~Slot();
    }
    free(buf_);
  }

  MPMCQueue(const MPMCQueue&) = delete;
  MPMCQueue& operator=(const MPMCQueue&) = delete;

  template <typename... Args>
  void emplace(Args&&... args) noexcept {
    static_assert(std::is_nothrow_constructible<T, Args&&...>::value,
                  "T must be nothrow construtible with Args&&...");
    auto const write_ticket = write_cursor_.fetch_add(1);
    auto& slot = slots_[idx(write_ticket)];
    while (turn(write_ticket) * 2 !=
           slot.turn.load(std::memory_order_acquire)) {
    }
    slot.construct(std::forward<Args>(args)...);
    slot.turn.store(turn(write_ticket) * 2 + 1, std::memory_order_release);
  }

  template <typename... Args>
  bool try_emplace(Args&&... args) noexcept {
    static_assert(std::is_nothrow_constructible<T, Args&&...>::value,
                  "T must be nothrow construtible with Args&&...");
    auto write_ticket = write_cursor_.load(std::memory_order_acquire);
    for (;;) {
      auto& slot = slots_[idx(write_ticket)];
      if (turn(write_ticket) * 2 == slot.turn.load(std::memory_order_acquire)) {
        if (write_cursor_.compare_exchange_strong(write_ticket,
                                                  write_ticket + 1)) {
          slot.construct(std::forward<Args>(args)...);
          slot.turn.store(turn(write_ticket) * 2 + 1,
                          std::memory_order_release);
          return true;
        }
      } else {
        auto const prev_write_ticket = write_ticket;
        write_ticket = write_cursor_.load(std::memory_order_acquire);
        if (prev_write_ticket == write_ticket) {
          return false;
        }
      }
    }
  }

  void push(const T& v) noexcept {
    static_assert(std::is_nothrow_copy_constructible<T>::value,
                  "T must be nothrow copy constructible");
    emplace(v);
  }

  template <typename P, typename = typename std::enable_if<
                            std::is_nothrow_constructible<T, P&&>::value>::type>
  void push(P&& v) noexcept {
    emplace(std::forward<P>(v));
  }

  bool try_push(const T& v) noexcept {
    static_assert(std::is_nothrow_copy_constructible<T>::value,
                  "T must be nothrow copy constructible");
    return try_emplace(v);
  }

  template <typename P, typename = typename std::enable_if<
                            std::is_nothrow_constructible<T, P&&>::value>::type>
  bool try_push(P&& v) noexcept {
    return try_emplace(std::forward<P>(v));
  }

  void pop(T& v) noexcept {
    auto const read_ticket = read_cursor_.fetch_add(1);
    auto& slot = slots_[idx(read_ticket)];
    while (turn(read_ticket) * 2 + 1 !=
           slot.turn.load(std::memory_order_acquire)) {
    }
    v = slot.move();
    slot.destroy();
    slot.turn.store(turn(read_ticket) * 2 + 2, std::memory_order_release);
  }

  bool try_pop(T& v) noexcept {
    auto read_ticket = read_cursor_.load(std::memory_order_acquire);
    for (;;) {
      auto& slot = slots_[idx(read_ticket)];
      if (turn(read_ticket) * 2 + 1 ==
          slot.turn.load(std::memory_order_acquire)) {
        if (read_cursor_.compare_exchange_strong(read_ticket,
                                                 read_ticket + 1)) {
          v = slot.move();
          slot.destroy();
          slot.turn.store(turn(read_ticket) * 2 + 2, std::memory_order_release);
          return true;
        }
      } else {
        auto prev_read_ticket = read_ticket;
        read_ticket = read_cursor_.load(std::memory_order_acquire);
        if (read_ticket == prev_read_ticket) {
          return false;
        }
      }
    }
  }

 private:
  constexpr size_t idx(size_t i) const noexcept { return i % capacity_; }

  constexpr size_t turn(size_t i) const noexcept { return i / capacity_; }

  struct Slot {
    ~Slot() noexcept {
      if (turn & 1) {
        destroy();
      }
    }

    template <typename... Args>
    void construct(Args&&... args) noexcept {
      static_assert(std::is_nothrow_constructible<T, Args&&...>::value,
                    "T must be nothrow constructible with Args&&...");
      new (&storage) T(std::forward<Args>(args)...);
    }

    void destroy() noexcept {
      static_assert(std::is_nothrow_destructible<T>::value,
                    "T must be nothrow destrcutible");
      reinterpret_cast<T*>(&storage)->~T();
    }

    T&& move() noexcept { return reinterpret_cast<T&&>(storage); }

    alignas(kCachelineSize) std::atomic<size_t> turn = {0};
    typename std::aligned_storage<sizeof(T), alignof(T)>::type storage;
  };

  const size_t capacity_;
  Slot* slots_;
  void* buf_;
  alignas(kCachelineSize) std::atomic<size_t> read_cursor_;
  alignas(kCachelineSize) std::atomic<size_t> write_cursor_;
};

}  // namespace lock_free
