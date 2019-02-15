#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <algorithm>
#include <vector>

namespace lock_free
{

class HazPtrRecords {
  HazPtrRecords* next_;
  std::atomic<bool> active_;

  HazPtrRecords()
      : next_(nullptr)
        ,active_(false)
  {}

  static std::atomic<HazPtrRecords*> head_;
  static std::atomic<size_t> list_len_;
  static thread_local std::vector<void*> retire_list_;
 public:
  void *p_hazard_;

  static HazPtrRecords* head() {
    return head_.load(std::memory_order_relaxed);
  }

  static HazPtrRecords* acquire() {
    HazPtrRecords* p = head_.load(std::memory_order_relaxed);
    bool expected = false;
    for (; p != nullptr; p = p->next_) {
      if (p->active_.load(std::memory_order_relaxed)
          || p->active_.compare_exchange_strong(expected, true)) {
        continue;
      }
      return p;
    }

    size_t old_len;
    do {
      old_len = list_len_.load(std::memory_order_relaxed);
    } while (!list_len_.compare_exchange_strong(old_len, old_len + 1));

    p = new HazPtrRecords();
    HazPtrRecords* old_head;
    do {
      old_head = head_.load(std::memory_order_relaxed);
      p->next_ = old_head;
    } while (!head_.compare_exchange_strong(old_head, p));
    return p;
  }

  static void scan() {
    HazPtrRecords* head = HazPtrRecords::head();
    std::vector<void *> hp;
    while(head) {
      void* p = head->p_hazard_;
      if (p) {
        hp.push_back(p);
        head = head->next_;
      }
    }
    std::sort(std::begin(hp), hp.end(hp), std::less<void*>());

    for (auto it = std::begin(retire_list_); it != std::end(retire_list_);) {
      if (!std::binary_search(std::begin(hp), std::end(hp), *it)) {
        delete *it;
        if (&*it != &retire_list_.back()) {
          *it = retire_list_.back();
        }
        retire_list_.pop_back();
      }  else {
        ++it;
      }
    }
  }
};

} /* lock_free */
