#pragma once

#include <unordered_map>
#include <utility>
#include <type_traits>
#include <optional>
#include <atomic>
#include <folly/synchronization/Hazptr.h>
#include <folly/logging/xlog.h>

namespace util
{
namespace Concurrent
{

template <typename Allocator>
class HazPtrDeleter {
 public:
  template <typename Node>
  void operator()(Node* node) {
    node->~Node();
    Allocator().deallocate(reinterpret_cast<uint8_t*>(node), sizeof(Node));
  }
};

template <typename Allocator>
class HazPtrBucketDeleter {
  size_t count_;
 public:
  HazPtrBucketDeleter(size_t count) : count_(count) {}
  HazPtrBucketDeleter() = default;
  template <typename Bucket>
  void operator()(Bucket* bucket) {
    bucket->destory(count_);
  }
};

template <
  typename Key,
  typename Value,
  typename Allocator,
  typename Enabled = void>
class ValueHolder {
 public:

  typedef std::pair<const Key, Value> value_type;

  explicit ValueHolder(const ValueHolder& other) : item_(other.item_) {}

  template <typename Arg, typename... Args>
  ValueHolder(std::piecewise_construct_t, Arg&& k, Args&&... args)
  : item_(
      std::piecewise_construct,
      std::forward_as_tuple(std::forward<Arg>(k)),
      std::forward_as_tuple(std::forward<Args>(args)...))
  {}

  value_type& get_item() {
    return item_;
  }

 private:
  value_type item_;
};

template <typename Key, typename Value, typename Allocator>
class ValueHolder<
  Key,
  Value,
  Allocator,
  std::enable_if_t<
    !std::is_nothrow_copy_constructible<Key>::value ||
    !std::is_nothrow_copy_constructible<Value>::value>> {
 public:
  typedef std::pair<const Key, Value> value_type;

  explicit ValueHolder(const ValueHolder& other){
    owned_ = false;
    item_ = other.item_;
  }

  template <typename Arg, typename... Args>
  ValueHolder(std::piecewise_construct_t, Arg&& k, Args&&... args) {
    item_ = reinterpret_cast<value_type*>(Allocator().allocate(sizeof(value_type)));
    new (item_) value_type(
        std::piecewise_construct,
        std::forward_as_tuple(std::forward<Arg>(k)),
        std::forward_as_tuple(std::forward<Args>(args)...));
  }

  ~ValueHolder() {
    if (owned_) {
      item_->~value_type();
      Allocator().deallocate(reinterpret_cast<uint8_t*>(item_), sizeof(value_type));
    }
  }

  value_type& get_item() {
    return *item_;
  }

 private:
  mutable bool owned_{true};
  value_type* item_;
};

template <
  typename Key,
  typename Value,
  typename Allocator>
class NodeT : public folly::hazptr_obj_base_linked<
                NodeT<Key, Value, Allocator>,
                std::atomic,
                HazPtrDeleter<Allocator>> {
 public:
  typedef std::pair<const Key, Value> value_type;

  explicit NodeT(NodeT* other) : item_(other->item_) {
    this->set_deleter(HazPtrDeleter<Allocator>());
    this->acquire_link_safe();
  }

  template <typename Arg, typename... Args>
  NodeT(Arg&& k, Args&&... args)
  : item_(
      std::piecewise_construct,
      std::forward<Arg>(k),
      std::forward<Args>(args)...) {
    this->set_deleter(HazPtrDeleter<Allocator>());
    this->acquire_link_safe();
  }

  void release() {
    this->unlink();
  }

  value_type& get_item() {
    return item_.get_item();
  }

  template <typename S>
  void push_links(bool m, S& s) {
    if (m) {
      auto p = next_.load(std::memory_order_acquire);
      if (p) {
        s.push(p);
      }
    }
  }

  std::atomic<NodeT*> next_{nullptr};
 private:
  ValueHolder<Key, Value, Allocator> item_;
};

template <
  typename Key,
  typename Value,
  uint8_t ShardBits = 0,
  typename HashFn = std::hash<Key>,
  typename KeyEqual = std::equal_to<Key>,
  typename Allocator = std::allocator<uint8_t>>
class alignas(64) ConcurrentHashMapSegment {
  enum class InsertType {
    DOES_NOT_EXISTS,
    MUST_EXISTS,
    ANY,
    MATCH,
  };
 public:
  typedef Key key_type;
  typedef Value mapped_type;
  typedef std::pair<const Key, Value> value_type;
  typedef std::size_t size_type;
  using Node = NodeT<Key, Value, Allocator>;
  class Iterator;

  ConcurrentHashMapSegment(
      size_t initial_buckets,
      float load_factor,
      size_t max_size)
      : load_factor_(load_factor)
        ,max_size_(max_size) {
    initial_buckets = folly::nextPowTwo(initial_buckets);
    DCHECK( max_size_ == 0
         || (folly::isPowTwo(max_size_)
         && (folly::popcount(max_size_ - 1) + ShardBits <= 32)));
    auto buckets = Buckets::create(initial_buckets);
    buckets_.store(buckets, std::memory_order_release);
    load_factor_nodes_ = initial_buckets * load_factor_;
    bucket_count_.store(initial_buckets, std::memory_order_relaxed);
  }

  ~ConcurrentHashMapSegment() {
    auto buckets = buckets_.load(std::memory_order_relaxed);
    auto count = bucket_count_.load(std::memory_order_relaxed);
    buckets->unlink_and_reclaim_node(count);
    buckets->destory(count);
  }

  size_t size() const noexcept {
    return size_;
  }

  bool empty() const noexcept {
    return size() == 0;
  }

  bool insert(Iterator& it, std::pair<key_type, mapped_type>&& foo) {
    return insert(it, std::move(foo.first), std::move(foo.second));
  }

  template <typename K, typename V>
  bool insert(Iterator& it, K&& k, V&& v) {
    auto node = (Node*)Allocator().allocate(sizeof(Node));
    new (node) Node(std::forward<K>(k), std::forward<V>(v));
    auto res = insert_internal(
        it,
        node->get_item().first,
        InsertType::DOES_NOT_EXISTS,
        [](const mapped_type&) {return false;},
        node,
        node);
    if (!res) {
      node->~Node();
      Allocator().deallocate((uint8_t*)node, sizeof(Node));
    }
    return res;
  }

  template <typename K, typename... Args>
  bool try_emplace(Iterator& it, K&& k, Args&&... args) {
    return insert_internal(it,
                           std::forward<K>(k),
                           InsertType::DOES_NOT_EXISTS,
                           [](const mapped_type&) {return false;},
                           nullptr,
                           std::forward<K>(k),
                           std::forward<Args>(args)...);
  }

  template <typename ...Args>
  bool emplace(Iterator& it, const key_type& k, Node* node) {
    return insert_internal(it,
                           k,
                           InsertType::DOES_NOT_EXISTS,
                           [](const mapped_type&){return false;},
                           node,
                           node);
  }

  template <typename K, typename V>
  bool insert_or_assign(Iterator& it, K&& k, V&& v) {
    auto node = static_cast<Node*>(Allocator().allocate(sizeof(Node)));
    new (node) Node(std::forward<K>(k), std::forward<V>(v));
    auto res = insert_internal(it,
                               k,
                               InsertType::ANY,
                               [](const mapped_type&){return false;},
                               node,
                               node);
    if (!res) {
      node->~Node();
      Allocator().deallocate(reinterpret_cast<uint8_t*>(node), sizeof(node));
    }
    return res;
  }

  template <typename K, typename V>
  bool assign(Iterator& it, K&& k, V&& v) {
    auto node = static_cast<Node*>(Allocator().allocate(sizeof(Node)));
    new (node) Node(std::forward<K>(k), std::forward<V>(v));
    auto res = insert_internal(it,
                               k,
                               InsertType::MUST_EXISTS,
                               [](const mapped_type&){return false;},
                               node,
                               node);
    if (!res) {
      node->~Node();
      Allocator().deallocate(reinterpret_cast<uint8_t*>(node), sizeof(Node));
    }
    return res;
  }

  template <typename K, typename V>
  bool assign_if_equal(Iterator& it,
                       K&& k,
                       const mapped_type& expected,
                       V&& desired) {
    auto node = reinterpret_cast<Node*>(Allocator().allocate(sizeof(Node)));
    new (node) Node(std::forward<K>(k), std::forward<V>(desired));
    auto res = insert_internal(it,
                               k,
                               InsertType::MATCH,
                               [&expected](const mapped_type& v) { return v == expected; },
                               node,
                               node);
    if (!res) {
      node->~Node();
      Allocator().deallocate(reinterpret_cast<uint8_t*>(node), sizeof(Node));
    }
    return res;
  }

  template <typename MatchFunc, typename... Args>
  bool insert_internal(
      Iterator& it,
      const key_type& k,
      InsertType type,
      MatchFunc match,
      Node* cur,
      Args&&... args) {
    auto hash = HashFn()(k);
    std::unique_lock<std::mutex> g(mutex_);

    size_t bcount = bucket_count_.load(std::memory_order_relaxed);
    auto buckets = buckets_.load(std::memory_order_relaxed);
    if (size_ >= load_factor_nodes_ && type == InsertType::DOES_NOT_EXISTS) {
      if (max_size_ && size_ << 1 > max_size_) {
        throw std::bad_alloc();
      }
      rehash(bcount << 1);
      buckets = buckets_.load(std::memory_order_relaxed);
      bcount = bucket_count_.load(std::memory_order_relaxed);
    }

    auto idx = get_idx(bcount, hash);
    auto head = &buckets->buckets_[idx]();
    auto node = head->load(std::memory_order_relaxed);
    auto headnode = node;
    auto prev = head;
    auto& hazbuckets = it.hazptrs_[0];
    auto& haznode = it.hazptrs_[1];
    hazbuckets.reset(buckets);
    while (node) {
      if (KeyEqual()(k, node->get_item().first)) {
        XLOG(INFO) << "FUCKED!";
        it.set_node(node, buckets, bcount, idx);
        haznode.reset(node);
        if (type == InsertType::MATCH) {
          if (!match(node->get_item().second)){
            return false;
          }
        }
        if (type == InsertType::DOES_NOT_EXISTS) {
          return false;
        } else {
          if (!cur) {
            cur = reinterpret_cast<Node*>(Allocator().allocate(sizeof(Node)));
            new (cur) Node(std::forward<Args>(args)...);
          }
          auto next = node->next_.load(std::memory_order_relaxed);
          cur->next_.store(next, std::memory_order_relaxed);
          if (next) {
            next->acquire_link();
          }
          prev->store(cur, std::memory_order_release);
          g.unlock();
          node->release();
          return true;
        }
      }
      prev = &node->next_;
      node = node->next_.load(std::memory_order_relaxed);
    }
    if (type != InsertType::DOES_NOT_EXISTS && type != InsertType::ANY) {
      haznode.reset();
      hazbuckets.reset();
      return false;
    }
    if (size_ >= load_factor_nodes_ && type == InsertType::ANY) {
      if (max_size_ && size_ << 1 > max_size_) {
        throw std::bad_alloc();
      }
      rehash(bcount << 1);
      buckets = buckets_.load(std::memory_order_relaxed);
      bcount <<= 1;
      hazbuckets.reset(buckets);
      idx = get_idx(bcount, hash);
      head = &buckets->buckets_[idx]();
      headnode = head->load(std::memory_order_relaxed);
    }
    ++size_;
    if (!cur) {
      DCHECK(type == InsertType::ANY || type == InsertType::DOES_NOT_EXISTS);
      cur = reinterpret_cast<Node*>(Allocator().allocate(sizeof(Node)));
      new (cur) Node(std::forward<Args>(args)...);
    }
    cur->next_.store(headnode, std::memory_order_relaxed);
    head->store(cur, std::memory_order_release);
    it.set_node(cur, buckets_, bcount, idx);
    return true;
  }

  void rehash(size_t bucket_count) {
    auto buckets = buckets_.load(std::memory_order_relaxed);
    auto newbuckets = Buckets::create(bucket_count);
    load_factor_nodes_ = bucket_count * load_factor_;
    auto oldcount = bucket_count_.load(std::memory_order_relaxed);
    for (size_t i = 0; i < bucket_count; ++i) {
      auto bucket = &buckets->buckets_[i]();
      auto node = bucket->load(std::memory_order_relaxed);
      if (!node) {
        continue;
      }
      auto h = HashFn()(node->get_item().first);
      auto idx = get_idx(bucket_count, h);
      auto lastrun = node;
      auto lastidx = idx;
      auto count = 0;
      auto last = node->next_.load(std::memory_order_relaxed);
      for (; last != nullptr;
           last = last->next_.load(std::memory_order_relaxed)) {
        auto k = get_idx(bucket_count, HashFn()(last->get_item().first));
        if (k != lastidx) {
          lastidx = k;
          lastrun = last;
          count = 0;
        }
        ++count;
      }
      lastrun->acquire_link();
      newbuckets->buckets_[lastidx]().store(lastrun, std::memory_order_relaxed);
      for (; node != lastrun;
           node = node->next_.load(std::memory_order_relaxed)) {
        auto newnode = reinterpret_cast<Node*>(Allocator().allocate(sizeof(Node)));
        new (newnode) Node(node);
        auto k = get_idx(bucket_count, HashFn()(node->get_item().first));
        auto prevhead = &newbuckets->buckets_[k]();
        newnode->next_.store(prevhead->load(std::memory_order_relaxed));
        prevhead->store(newnode, std::memory_order_relaxed);
      }
    }
    auto oldbuckets = buckets_.load(std::memory_order_relaxed);
    seq_lock_.fetch_add(1, std::memory_order_release);
    bucket_count_.store(bucket_count, std::memory_order_release);
    buckets_.store(newbuckets, std::memory_order_release);
    seq_lock_.fetch_add(1, std::memory_order_release);
    oldbuckets->retire(HazPtrBucketDeleter<Allocator>(oldcount));
  }

  bool find(Iterator& res, const key_type& k) {
    auto& hazcurr = res.hazptrs_[1];
    auto& haznext = res.hazptrs_[2];
    auto h = HashFn()(k);
    size_t bcount;
    Buckets* buckets;
    get_bucket_and_count(bcount, buckets, res.hazptrs_[0]);

    auto idx = get_idx(bcount, h);
    auto prev = &buckets->buckets_[idx]();
    auto node = hazcurr.get_protected(*prev);
    while (node) {
      if (KeyEqual()(k, node->get_item().first)) {
        res.set_node(node, buckets, bcount, idx);
        return true;
      }
      node = haznext.get_protected(node->next_);
      hazcurr.swap(haznext);
    }
    return false;
  }

  size_type erase(const key_type& k) {
    return erase_internal(k, nullptr);
  }

  size_type erase_internal(const key_type& k, Iterator* it) {
    Node* node{nullptr};
    auto h = HashFn()(k);
    {
      std::lock_guard<std::mutex> g(mutex_);

      size_t bcount = bucket_count_.load(std::memory_order_relaxed);
      auto buckets = buckets_.load(std::memory_order_relaxed);
      auto idx = get_idx(bcount, h);
      auto head = &buckets->buckets_[idx]();
      node = head->load(std::memory_order_relaxed);
      Node* prev{nullptr};
      while (node) {
        if (KeyEqual()(k, node->get_item.first)) {
          auto next = node->next_.load(std::memory_order_relaxed);
          if (next) {
            next->acquire_link();
          }
          if (prev) {
            prev->next_.store(next, std::memory_order_release);
          } else {
            head->store(next, std::memory_order_release);
          }

          if (it) {
            it->hazptrs_[0].reset(buckets);
            it->set_node(
                node->next_.load(std::memory_order_acquire),
                buckets,
                bcount,
                idx);
            it->next();
          }
          size_--;
          break;
        }
        prev = node;
        node = node->next_.load(std::memory_order_relaxed);
      }
    }

      if (node) {
        node->release();
        return 1;
      }
      return 0;
  }

  class Buckets
      : public folly::hazptr_obj_base<
                        Buckets,
                        std::atomic,
                        HazPtrBucketDeleter<Allocator>> {
    using BucketRoot = folly::hazptr_root<Node, std::atomic>;
    Buckets() {}
    ~Buckets() {}
   public:
    static Buckets* create(size_t count) {
      auto buf = Allocator().allocate(sizeof(Buckets) + sizeof(BucketRoot) * count);
      auto buckets = new (buf) Buckets();
      for (size_t i = 0; i < count; ++i) {
        new (&buckets->buckets_[i]) BucketRoot;
      }
      return buckets;
    }

    void destory(size_t count) {
      for (size_t i = 0; i < count; ++i) {
        buckets_[i].~BucketRoot();
      }
      this->~Buckets();
      Allocator().deallocate(
          reinterpret_cast<uint8_t*>(this),
          sizeof(BucketRoot) * count + sizeof(this));
    }

    void unlink_and_reclaim_node(size_t count) {
      for (size_t i = 0; i < count; ++i) {
        auto node = buckets_[i]().load(std::memory_order_relaxed);
        if (node) {
          buckets_[i]().store(nullptr, std::memory_order_relaxed);
          while (node) {
            auto next = node->next_.load(std::memory_order_relaxed);
            if (next) {
              node->next_.store(nullptr, std::memory_order_relaxed);
            }
            node->unlink_and_reclaim_unchecked();
            node = next;
          }
        }
      }
    }

    BucketRoot buckets_[0];
  };
 public:
  class Iterator {
   public:
    inline Iterator() {}
    inline explicit Iterator(std::nullptr_t) : hazptrs_(nullptr) {}

    void set_node(Node* node, Buckets* buckets, size_t bucket_count, uint64_t idx) {
      node_ = node;
      buckets_ = buckets;
      bucket_count_ = bucket_count;
      idx_ = idx;
    }

    const value_type& operator*() const {
      DCHECK(node_);
      return node_->get_item();
    }

    const value_type* operator->() const {
      DCHECK(node_);
      return &(node_->get_item());
    }

    const Iterator& operator++() {
      DCHECK(node_);
      node_ = hazptrs_[2].get_protected(node_->next_);
      hazptrs_[1].swap(hazptrs_[2]);
      if (!node_) {
        ++idx_;
        next();
      }
      return *this;
    }

    void next() {
      while (!node_) {
        if (idx_ >= bucket_count_) {
          break;
        }
        DCHECK(buckets_);
        node_ = hazptrs_[1].get_protected(buckets_->buckets_[idx_]());
        if (node_) {
          break;
        }
        ++idx_;
      }
    }

    bool operator==(const Iterator& other) const {
      return node_ == other.node_;
    }

    bool operator!=(const Iterator& other) const {
      return !(*this == other);
    }

    Iterator& operator=(const Iterator&) = delete;
    Iterator& operator==(Iterator&& other) noexcept {
      if (this != &other) {
        hazptrs_ = std::move(other.hazptrs_);
        node_ = std::exchange(other.node_, nullptr);
        buckets_ = std::exchange(other.buckets_, nullptr);
        bucket_count_ = std::exchange(other.bucket_count_, 0);
        idx_ = std::exchange(other.idx_, 0ul);
      }
      return *this;
    }

    Iterator(const Iterator&) = delete;
    Iterator(Iterator&& other) noexcept
      : hazptrs_(std::move(other.hazptrs_))
      , node_(std::exchange(other.node_, nullptr))
      , buckets_(std::exchange(other.buckets_, nullptr))
      , bucket_count_(std::exchange(other.bucket_count_, 0))
      , idx_(std::exchange(other.idx_, 0)) {}


    folly::hazptr_array<3, std::atomic> hazptrs_;
   private:
    Node* node_{nullptr};
    Buckets* buckets_{nullptr};
    size_t bucket_count_{0};
    uint64_t idx_{0};
  };

 private:
  uint64_t get_idx(size_t bucket_count, size_t hash) {
    return (hash >> ShardBits) & (bucket_count - 1);
  }

  void get_bucket_and_count(size_t& bucket_count,
                            Buckets*& buckets,
                            folly::hazptr_holder<std::atomic>& hazptr) {
    while (true) {
      auto seq_lock = seq_lock_.load(std::memory_order_acquire);
      bucket_count = bucket_count_.load(std::memory_order_acquire);
      buckets = hazptr.get_protected(buckets_);
      auto seq_lock2 = seq_lock_.load(std::memory_order_acquire);
      if (!(seq_lock & 1) && (seq_lock == seq_lock2)) {
        break;
      }
    }
    DCHECK(buckets);
  }

  std::mutex mutex_;
  float load_factor_;
  size_t load_factor_nodes_;
  size_t size_{0};
  size_t const max_size_;
  alignas(64) std::atomic<Buckets*> buckets_{nullptr};
  std::atomic<int64_t> seq_lock_{0};
  std::atomic<size_t> bucket_count_;
};

template <
  typename Key,
  typename Value,
  typename HashFn = std::hash<Key>,
  typename KeyEqual = std::equal_to<Key>,
  typename Allocator = std::allocator<uint8_t>,
  uint8_t ShardBits = 8
>
class SimpleConcurrentHashMap {
  using SegmentT = ConcurrentHashMapSegment<
      Key,
      Value,
      ShardBits,
      HashFn,
      KeyEqual,
      Allocator>;

  static constexpr uint64_t num_shards = 1 << ShardBits;
  float load_factor_ = 1.05;
 public:
  class ConstIterator;
  using key_type = Key;
  using mapped_type = Value;
  using value_type = std::pair<const key_type, mapped_type>;
  using size_type = size_t;
  using hasher = HashFn;
  using key_equal = KeyEqual;
  using const_iterator = ConstIterator;

  explicit SimpleConcurrentHashMap(size_t size = 8, size_t max_size = 0) {
    size_ = folly::nextPowTwo(size);
    if (max_size != 0) {
      max_size_ = folly::nextPowTwo(max_size);
    }
    DCHECK(max_size_ == 0 || max_size_ > size_);
    for (uint64_t i = 0; i < num_shards; ++i) {
      segments_[i].store(nullptr, std::memory_order_relaxed);
    }
  }

  SimpleConcurrentHashMap(SimpleConcurrentHashMap&& other) noexcept {
    for (uint64_t i = 0; i < num_shards; ++i) {
      segments_[i].store(other.segments_[i].load(std::memory_order_relaxed),
                         std::memory_order_relaxed);
      other.segments_[i].store(nullptr, std::memory_order_relaxed);
    }
  }

  SimpleConcurrentHashMap& operator=(SimpleConcurrentHashMap&& other) {
    for (uint64_t i = 0; i < num_shards; ++i) {
      auto seg = segments_[i].load(std::memory_order_relaxed);
      if (seg) {
        seg->~SegmentT();
        Allocator().deallocate(static_cast<uint8_t*>(seg), sizeof(SegmentT));
      }
      segments_[i].store(other.segments_[i].load(std::memory_order_relaxed),
                         std::memory_order_relaxed);
      other.segments_[i].store(nullptr, std::memory_order_relaxed);
    }
    size_ = other.size_;
    max_size_ = other.max_size_;
    return *this;
  }

  ~SimpleConcurrentHashMap() {
    for (uint64_t i = 0; i < num_shards; ++i) {
      auto seg = segments_[i].load(std::memory_order_relaxed);
      if (seg) {
        seg->~SegmentT();
        Allocator().deallocate(reinterpret_cast<uint8_t*>(seg), sizeof(SegmentT));
      }
    }
  }

  bool empty() const noexcept {
    for (uint64_t i = 0; i < num_shards; ++i) {
      auto seg = segments_[i].load(std::memory_order_relaxed);
      if (seg) {
        if (!seg->empty()) {
          return false;
        }
      }
    }
    return true;
  }

  ConstIterator find(const key_type& k) const {
    auto seg_idx = pick_segment(k);
    ConstIterator res(this, seg_idx);
    auto segment = segments_[seg_idx].load(std::memory_order_acquire);
    if (!segment || !segment->find(res.it_, k)) {
      res.segment_ = num_shards;
    }
    return res;
  }

  ConstIterator cend() const noexcept {
    return ConstIterator(num_shards);
  }

  ConstIterator cbegin() const noexcept {
    return ConstIterator(this);
  }

  ConstIterator end() const noexcept {
    return cend();
  }

  ConstIterator begin() const noexcept {
    return cbegin();
  }

  std::pair<ConstIterator, bool> insert(
      std::pair<key_type, mapped_type>&& foo) {
    auto seg_idx = pick_segment(foo.first);
    std::pair<ConstIterator, bool> res(
        std::piecewise_construct,
        std::forward_as_tuple(this, seg_idx),
        std::forward_as_tuple(false));
    res.second = ensure_segment(seg_idx)->insert(res.first.it_, std::move(foo));
    return res;
  }

  template <typename K, typename ...Args>
  std::pair<ConstIterator, bool> try_emplace(K&& k, Args&&... args) {
    auto seg_idx = pick_segment(k);
    std::pair<ConstIterator, bool> res(
        std::piecewise_construct,
        std::forward_as_tuple(this, seg_idx),
        std::forward_as_tuple(false));
    res.second = ensure_segment(seg_idx)->insert(
        res.first.it_, std::forward<K>(k), std::forward<Args>(args)...);
    return res;
  }

  template <typename... Args>
  std::pair<ConstIterator, bool> emplace(Args&&... args) {
    using Node = typename SegmentT::Node;
    auto node = reinterpret_cast<Node*>(Allocator().allocate(sizeof(Node)));
    new (node) Node(std::forward<Args>(args)...);
    auto seg_idx = pick_segment(node->get_item().first);
    XLOG(INFO) << seg_idx;
    std::pair<ConstIterator, bool> res(
        std::piecewise_construct,
        std::forward_as_tuple(this, seg_idx),
        std::forward_as_tuple(false));
    res.second = ensure_segment(seg_idx)->emplace(
        res.first.it_, node->get_item().first, node
        );
    if (!res.second) {
      node->~Node();
      Allocator().deallocate(reinterpret_cast<uint8_t*>(node), sizeof(Node));
    }
    return res;
  }

  template <typename K, typename V>
  std::pair<ConstIterator, bool> insert_or_assign(K&& k, V&& v) {
    auto seg_idx = pick_segment(k);
    std::pair<ConstIterator, bool> res(
        std::piecewise_construct,
        std::forward_as_tuple(this, seg_idx),
        std::forward_as_tuple(false)
        );
    res.second = ensure_segment(seg_idx)->insert_or_assign(
        res.first, std::forward<K>(k), std::forward<V>(v)
        );
    return res;
  }

  template <typename K, typename V>
  std::optional<ConstIterator> assign(K&& k, V&& v) {
    auto seg_idx = pick_segment(k);
    ConstIterator res(this, seg_idx);
    auto seg = segments_[seg_idx].load(std::memory_order_acquire);
    if (!seg) {
      return {};
    } else {
      auto r =
          seg->assign(res.it_, std::forward<K>(k), std::forward<V>(v));
      if (!r) {
        return {};
      }
    }
    return std::move(res);
  }

  template <typename K, typename V>
  std::optional<ConstIterator>
  assign_if_equal(K&& k, const value_type& expected, V&& desired) {
    auto seg_idx = pick_segment(k);
    ConstIterator res(this, seg_idx);
    auto seg = segments_[seg_idx].load(std::memory_order_acquire);
    if (!seg) {
      return {};
    } else {
      auto r = seg->assign_if_equal(
          res.it_,
          std::forward<K>(k),
          expected,
          std::forward<V>(desired)
          );
      if (!r) {
        return {};
      }
    }
    return std::move(res);
  }

  const value_type operator[](const key_type& k) {
    auto item = insert(k, value_type());
    return item.first->second;
  }

  const value_type at(const key_type& k) {
    auto item = find(k);
    if (item == cend()) {
      throw std::out_of_range("at() : value out of range");
    }
    return item->second;
  }

  size_type erase(const key_type& k) {
    auto seg_idx = pick_segment(k);
    auto seg = segments_[seg_idx].load(std::memory_order_acquire);
    if (!seg) {
      return 0;
    } else {
      return seg->erase(k);
    }
  }

  ConstIterator erase(ConstIterator& pos) {
    auto seg_idx = pick_segment(pos->first);
    ConstIterator res(this, seg_idx);
    ensure_segment(seg_idx)->erase(res.it_, pos.it_);
    res.next();
    return res;
  }

  class ConstIterator {
   public:
    friend class SimpleConcurrentHashMap;

    const value_type& operator*() const {
      return *it_;
    }

    const value_type* operator->() const {
      return &*it_;
    }

    ConstIterator& operator++() {
      ++it_;
      next();
      return *this;
    }

    bool operator==(const ConstIterator& o) const {
      return it_ == o.it_ && segment_ == o.segment_;
    }

    bool operator!=(const ConstIterator& o) const {
      return !(*this == o);
    }

    ConstIterator& operator=(const ConstIterator&) = delete;

    ConstIterator& operator=(ConstIterator&& o) noexcept {
      if (this != &o) {
        it_ = std::move(o.it_);
        segment_ = std::exchange(o.segment_, uint64_t(num_shards));
        parent_ = std::exchange(o.parent_, nullptr);
      }
      return *this;
    }

    ConstIterator(const ConstIterator&) = delete;

    ConstIterator(ConstIterator&& o)
        : it_(std::move(o.it_))
          , segment_(std::exchange(o.segment_, uint64_t(num_shards)))
          , parent_(std::exchange(o.parent_, nullptr)) {}

    ConstIterator(const SimpleConcurrentHashMap* parent, uint64_t segment)
        : parent_(parent), segment_(segment) {}


   private:
    explicit ConstIterator(const SimpleConcurrentHashMap* parent)
        : it_(parent->ensure_segment(0).cbegin())
          , segment_(0)
          , parent_(parent) {
            next();
    }

    explicit ConstIterator(uint64_t shard)
        : it_(nullptr)
          , segment_(shard) {}

    void next() {
      while (it_ == parent_->ensure_segment(segment_)->cend() &&
             segment_ < parent_->num_shards) {
        SegmentT* seg{nullptr};
        while (!seg) {
          segment_++;
          seg = parent_->segments_[segment_].load(std::memory_order_acquire);
          if (seg < parent_->num_shards) {
            if (!seg) {
              continue;
            }
            it_ = seg->cbegin();
          }
          break;
        }
      }
    }

    typename SegmentT::Iterator it_;
    uint64_t segment_;
    const SimpleConcurrentHashMap* parent_;
  };

  void clear() {
    for (uint64_t i = 0; i < num_shards; ++i) {
      auto seg = segments_[i].load(std::memory_order_acquire);
      if (seg) {
        seg->clear();
      }
    }
  }

  void reserve(size_t count) {
    count = count >> ShardBits;
    for (uint64_t i = 0; i < num_shards; ++i) {
      auto seg = segments_[i].load(std::memory_order_acquire);
      if (seg) {
        seg->rehash(count);
      }
    }
  }

  size_t size() const noexcept {
    size_t res = 0;
    for (uint64_t i = 0; i < num_shards; ++i) {
      auto seg = segments_[i].load(std::memory_order_acquire);
      if (seg) {
        res += seg->size();
      }
    }
    return res;
  }

  float max_load_factor() const noexcept {
    return load_factor_;
  }

  void max_load_factor(float load_factor) {
    for (uint64_t i = 0; i < num_shards; ++i) {
      auto seg = segments_[i].load(std::memory_order_acquire);
      if (seg) {
        seg->max_load_factor(load_factor);
      }
    }
  }

 private:

  size_t pick_segment(const key_type& k) const {
    auto h = hasher()(k);
    return h & (num_shards - 1);
  }

  SegmentT* ensure_segment(uint64_t i) const {
    SegmentT* seg = segments_[i].load(std::memory_order_relaxed);
    if (!seg) {
      SegmentT* new_seg = reinterpret_cast<SegmentT*>(Allocator().allocate(sizeof(SegmentT)));
      new (new_seg)
          SegmentT(size_ >> ShardBits, load_factor_, max_size_ >> ShardBits);

      if (!segments_[i].compare_exchange_strong(seg, new_seg)) {
        new_seg->~SegmentT();
        Allocator().deallocate(reinterpret_cast<uint8_t*>(new_seg), sizeof(SegmentT));
      } else {
        seg = new_seg;
      }
    }
    return seg;
  }

  mutable std::atomic<SegmentT*> segments_[num_shards];
  size_t size_{0};
  size_t max_size_{0};
};

} /* Concurrent */
} /*  util */
