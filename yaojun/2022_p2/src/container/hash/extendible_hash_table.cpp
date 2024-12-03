//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  // 初始化：全局深度为0，局部深度为0，桶数量为1
  dir_.push_back(std::make_shared<Bucket>(bucket_size, 0));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  /* 注意：这里的value是出参 */
  std::lock_guard<std::mutex> guard(latch_);

  auto index = IndexOf(key);
  std::shared_ptr<Bucket> bucket = dir_[index];
  // 拿到对应的桶
  if (bucket == nullptr) {
    return false;
  }
  for (auto &item : bucket->GetItems()) {
    if (item.first == key) {
      value = item.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::lock_guard<std::mutex> guard(latch_);

  auto index = IndexOf(key);
  std::shared_ptr<Bucket> bucket = dir_[index];

  std::list<int>::iterator pos;
  auto &items = bucket->GetItems();

  for (const auto &item : items) {
    if (item.first == key) {
      items.remove(item);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::lock_guard<std::mutex> guard(latch_);

  while (dir_[IndexOf(key)]->IsFull()) {
    int id = IndexOf(key);
    auto target_bucket = dir_[id];
    // 满了
    num_buckets_++;
    if (target_bucket->GetDepth() == GetGlobalDepthInternal()) {
      // 1. 局部深度等于全局深度：先扩容
      global_depth_++;
      int capacity = dir_.size();
      dir_.resize(capacity << 1);
      for (int idx = 0; idx < capacity; idx++) {
        dir_[idx + capacity] = dir_[idx];
      }
    }
    // 分裂
    int mask = 1 << target_bucket->GetDepth();
    auto zero_bucket = std::make_shared<Bucket>(bucket_size_, target_bucket->GetDepth() + 1);
    auto one_bucket = std::make_shared<Bucket>(bucket_size_, target_bucket->GetDepth() + 1);
    for (const auto &item : target_bucket->GetItems()) {
      size_t hashkey = std::hash<K>()(item.first);
      if ((hashkey & mask) != 0U) {
        one_bucket->Insert(item.first, item.second);
      } else {
        zero_bucket->Insert(item.first, item.second);
      }
    }

    for (size_t i = 0; i < dir_.size(); i++) {
      if (dir_[i] == target_bucket) {  // 这是目标桶，因为是2倍扩容，并且是相同内容，所以这个条件会触发两次
        if ((i & mask) != 0U) {
          dir_[i] = one_bucket;
        } else {
          dir_[i] = zero_bucket;
        }
      }
    }
  }

  // 桶没满
  auto index = IndexOf(key);
  auto target_bucket = dir_[index];

  // 更新
  for (auto &item : target_bucket->GetItems()) {
    if (item.first == key) {
      item.second = value;
      return;
    }
  }
  // 插入
  target_bucket->Insert(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size) {
  depth_ = depth;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const auto &p : list_) {
    if (p.first == key) {
      value = p.second;
      return true;
    }
  }
  LOG_INFO("enter bucket find");
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  list_.emplace_back(key, value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;

template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;

template class ExtendibleHashTable<int, int>;

// test purpose
template class ExtendibleHashTable<int, std::string>;

template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
