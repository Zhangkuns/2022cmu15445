//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.h
//
// Identification: src/include/container/hash/extendible_hash_table.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * extendible_hash_table.h
 *
 * Implementation of in-memory hash table using extendible hashing
 */

#pragma once
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <utility>
#include <vector>
#include "container/hash/hash_table.h"
namespace bustub {

/**
 * ExtendibleHashTable implements a hash table using the extendible hashing algorithm.
 * @tparam K key type
 * @tparam V value type
 */
template <typename K, typename V>
class ExtendibleHashTable : public HashTable<K, V> {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Create a new ExtendibleHashTable.
   * @param bucket_size: fixed size for each bucket
   */
  explicit ExtendibleHashTable(size_t bucket_size) {
    global_depth_ = 0;
    bucket_size_ = bucket_size;
    num_buckets_ = 1;
    dir_.push_back(std::make_shared<Bucket>(bucket_size_, global_depth_));
  }

  /**
   * @brief Get the global depth of the directory.
   * @return The global depth of the directory.
   */
  auto GetGlobalDepth() const -> int;

  /**
   * @brief Get the local depth of the bucket that the given directory index points to.
   * @param dir_index The index in the directory.
   * @return The local depth of the bucket.
   */
  auto GetLocalDepth(int dir_index) const -> int;

  /**
   * @brief Get the number of buckets in the directory.
   * @return The number of buckets in the directory.
   */
  auto GetNumBuckets() const -> int;

  /**
   * Acquire a write directory latch.
   */
  void DirWLock() { dir_mutex_.lock(); }

  /**
   * @brief Release a write directory latch.
   */
  void DirWUnlock() { dir_mutex_.unlock(); }

  /**
   * @brief Acquire a read directory latch.
   */
  void DirRLock() { dir_mutex_.lock_shared(); }

  /**
   * @brief Release a read directory latch.
   */
  void DirRUnlock() { dir_mutex_.unlock_shared(); }

  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Find the value associated with the given key.
   *
   * Use IndexOf(key) to find the directory index the key hashes to.
   *
   * @param key The key to be searched.
   * @param[out] value The value associated with the key.
   * @return True if the key is found, false otherwise.
   */
  auto Find(const K &key, V &value) -> bool override {
    std::scoped_lock<std::mutex> lock(latch_);
    size_t index = IndexOf(key);
    int current_mask = dir_[index]->GetLocalMask();
    std::shared_ptr<Bucket> current_ptr = dir_[index];
    if (current_ptr->GetDepth() == 0) {
      return current_ptr->Find(key, value);
    }
    while ((current_mask & std::hash<K>()(key)) != current_ptr->GetCommonBits()) {
      auto next_ptr = current_ptr->GetNextBucket();
      if (!next_ptr) {
        return false;  // No next bucket, key not found
      }
      current_mask = dir_[index]->GetLocalMask();
      current_ptr = current_ptr->GetNextBucket();
    }
    return current_ptr->Find(key, value);
  };

  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Insert the given key-value pair into the hash table.
   * If a key already exists, the value should be updated.
   * If the bucket is full and can't be inserted, do the following steps before retrying:
   *    1. If the local depth of the bucket is equal to the global depth,
   *        increment the global depth and double the size of the directory.
   *    2. Increment the local depth of the bucket.
   *    3. Split the bucket and redistribute directory pointers & the kv pairs in the bucket.
   *
   * @param key The key to be inserted.
   * @param value The value to be inserted.
   */
  void Insert(const K &key, const V &value) override {
    std::scoped_lock<std::mutex> lock(latch_);
    while (true) {
      std::shared_ptr<Bucket> current_ptr = dir_[IndexOf(key)];
      if (!(current_ptr->IsFull())) {  // bucket is not full
        if (current_ptr->Insert(key, value)) {
          return;
        }
      } else {  // bucket is full
        if (current_ptr->GetDepth() == global_depth_) {
          /**Double the directory*/
          int original_size = dir_.size();
          // Double the size of the directory and update pointers
          for (int i = 0; i < original_size; ++i) {
            dir_.push_back(dir_[i]);
          }
          global_depth_++;  // Increment global depth after fully updating the directory
          /**Spilt the Bucket*/
          num_buckets_ = num_buckets_ + 1;
          int original_mask = (1 << dir_[IndexOf(key)]->GetDepth()) - 1;
          auto original_bucket_underline = IndexOf(key) & original_mask;  // 原始桶的下标
          dir_[IndexOf(key)]->IncrementDepth();
          // split the bucket and redistribute directory pointers & the kv pairs in the bucket.
          int new_mask = (1 << dir_[IndexOf(key)]->GetDepth()) - 1;
          auto new_bucket_underline =
              original_bucket_underline + (1 << (dir_[IndexOf(key)]->GetDepth() - 1));  // 新桶的下标
          dir_[new_bucket_underline] = std::make_shared<Bucket>(bucket_size_, dir_[IndexOf(key)]->GetDepth());
          /** reexamine the original bucket
           * Be cautious with iterator invalidation. When you remove an item from the list
           * you're currently iterating over (dir_[original_bucket_underline]->Remove(...)),
           * it invalidates the iterator. This can lead to undefined behavior.
           */
          // get the original bucket's items
          std::list<std::pair<K, V>> original_bucket_list = dir_[original_bucket_underline]->GetItems();
          for (auto original_bucket_iterator = original_bucket_list.begin();
               original_bucket_iterator != original_bucket_list.end(); original_bucket_iterator++) {
            if ((IndexOf(original_bucket_iterator->first) & new_mask) == new_bucket_underline) {
              dir_[new_bucket_underline]->Insert(original_bucket_iterator->first, original_bucket_iterator->second);
              dir_[original_bucket_underline]->Remove(original_bucket_iterator->first);
            }
          }
          // update the next pointer
          dir_[new_bucket_underline]->SetNext(dir_[original_bucket_underline]->GetNextBucket());
          dir_[original_bucket_underline]->SetNext(dir_[new_bucket_underline]);
          // update the common bits
          dir_[original_bucket_underline]->UpdateCommonBits(original_bucket_underline);
          dir_[new_bucket_underline]->UpdateCommonBits(new_bucket_underline);
          // rehash the directory
          for (size_t i = 0; i < dir_.size(); i++) {
            if ((i & new_mask) == new_bucket_underline) {
              dir_[i] = std::shared_ptr<Bucket>(dir_[new_bucket_underline]);
            }
          }
        }
        if (current_ptr->GetDepth() < global_depth_) {
          /**Spilt the Bucket*/
          num_buckets_ = num_buckets_ + 1;
          int original_mask = (1 << dir_[IndexOf(key)]->GetDepth()) - 1;
          auto original_bucket_underline = IndexOf(key) & original_mask;  // 原始桶的下标
          dir_[IndexOf(key)]->IncrementDepth();
          // split the bucket and redistribute directory pointers & the kv pairs in the bucket.
          int new_mask = (1 << dir_[IndexOf(key)]->GetDepth()) - 1;
          auto new_bucket_underline =
              original_bucket_underline + (1 << (dir_[IndexOf(key)]->GetDepth() - 1));  // 新桶的下标
          dir_[new_bucket_underline] = std::make_shared<Bucket>(bucket_size_, dir_[IndexOf(key)]->GetDepth());
          /** reexamine the original bucket
           * Be cautious with iterator invalidation. When you remove an item from the list
           * you're currently iterating over (dir_[original_bucket_underline]->Remove(...)),
           * it invalidates the iterator. This can lead to undefined behavior.
           */
          // get the original bucket's items
          std::list<std::pair<K, V>> original_bucket_list = dir_[original_bucket_underline]->GetItems();
          for (auto original_bucket_iterator = original_bucket_list.begin();
               original_bucket_iterator != original_bucket_list.end(); original_bucket_iterator++) {
            if ((IndexOf(original_bucket_iterator->first) & new_mask) == new_bucket_underline) {
              dir_[new_bucket_underline]->Insert(original_bucket_iterator->first, original_bucket_iterator->second);
              dir_[original_bucket_underline]->Remove(original_bucket_iterator->first);
            }
          }
          // update the next pointer
          dir_[new_bucket_underline]->SetNext(dir_[original_bucket_underline]->GetNextBucket());
          dir_[original_bucket_underline]->SetNext(dir_[new_bucket_underline]);
          // update the common bits
          dir_[original_bucket_underline]->UpdateCommonBits(original_bucket_underline);
          dir_[new_bucket_underline]->UpdateCommonBits(new_bucket_underline);
          // rehash the directory
          for (size_t i = 0; i < dir_.size(); i++) {
            if ((i & new_mask) == new_bucket_underline) {
              dir_[i] = std::shared_ptr<Bucket>(dir_[new_bucket_underline]);
            }
          }
        }
      }
    }
  };

  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Given the key, remove the corresponding key-value pair in the hash table.
   * Shrink & Combination is not required for this project
   * @param key The key to be deleted.
   * @return True if the key exists, false otherwise.
   */
  auto Remove(const K &key) -> bool override {
    std::scoped_lock<std::mutex> lock(latch_);
    size_t index = IndexOf(key);
    int current_mask = dir_[index]->GetLocalMask();
    std::shared_ptr<Bucket> current_ptr = dir_[index];
    while (((current_mask & std::hash<K>()(key)) != current_ptr->GetCommonBits()) && current_ptr->GetDepth() != 0) {
      auto next_ptr = current_ptr->GetNextBucket();
      if (!next_ptr) {
        return false;  // No next bucket, key not found
      }
      current_mask = dir_[index]->GetLocalMask();
      current_ptr = current_ptr->GetNextBucket();
    }
    return current_ptr->Remove(key);
  };

  /**
   * Bucket class for each hash table bucket that the directory points to.
   */
  class Bucket {
   public:
    explicit Bucket(size_t size, int depth = 0) {
      size_ = size;
      depth_ = depth;
      next_bucket_ = nullptr;
    }

    /** @brief Check if a bucket is full. */
    inline auto IsFull() const -> bool { return list_.size() == size_; }

    /** @brief Get the local depth of the bucket. */
    inline auto GetDepth() const -> int { return depth_; }

    /** @brief Increment the local depth of a bucket. */
    inline void IncrementDepth() { depth_++; }

    inline auto GetItems() -> std::list<std::pair<K, V>> & { return list_; }

    /**Acquire a write latch.*/
    void WLock() { busket_mutex_.lock(); }

    /**Release a write latch.*/
    void WUnlock() { busket_mutex_.unlock(); }

    /**Acquire a read latch.*/
    void RLock() { busket_mutex_.lock_shared(); }

    /**Release a read latch.*/
    void RUnlock() { busket_mutex_.unlock_shared(); }

    /**Get the local mask of the Bucket*/
    auto GetLocalMask() -> int { return (1 << depth_) - 1; }

    /**Get the common bits*/
    auto GetCommonBits() -> std::optional<int> {
      if (depth_ == 0) {
        return std::nullopt;  // Indicates no value
      }
      return common_bits_;
    }

    /**Update the common bits*/
    void UpdateCommonBits(int new_common_bits) { common_bits_ = new_common_bits; }

    /**Get the pointer of the next Bucket*/
    auto GetNextBucket() -> std::shared_ptr<Bucket> { return next_bucket_; }

    /**Set the pointer of the next Bucket*/
    void SetNext(std::shared_ptr<Bucket> next_) { next_bucket_ = next_; }

    /**
     *
     * TODO(P1): Add implementation
     *
     * @brief Find the value associated with the given key in the bucket.
     * @param key The key to be searched.
     * @param[out] value The value associated with the key.
     * @return True if the key is found, false otherwise.
     */
    auto Find(const K &key, V &value) -> bool {
      if (list_.empty()) {
        return false;
      }
      for (auto it = list_.begin(); it != list_.end(); it++) {
        if (it->first == key) {
          value = it->second;
          return true;
        }
      }
      return false;
    };

    /**
     *
     * TODO(P1): Add implementation
     *
     * @brief Given the key, remove the corresponding key-value pair in the bucket.
     * @param key The key to be deleted.
     * @return True if the key exists, false otherwise.
     */
    auto Remove(const K &key) -> bool {
      if (list_.empty()) {
        return false;
      }
      for (auto it = list_.begin(); it != list_.end(); it++) {
        if (it->first == key) {
          list_.erase(it);
          return true;
        }
      }
      return false;
    };

    /**
     *
     * TODO(P1): Add implementation
     *
     * @brief Insert the given key-value pair into the bucket.
     *      1. If a key already exists, the value should be updated.
     *      2. If the bucket is full, do nothing and return false.
     * @param key The key to be inserted.
     * @param value The value to be inserted.
     * @return True if the key-value pair is inserted, false otherwise.
     */
    auto Insert(const K &key, const V &value) -> bool {
      if (IsFull()) {
        return false;
      }
      for (auto it = list_.begin(); it != list_.end(); it++) {
        if (it->first == key) {
          it->second = value;
          return true;
        }
      }
      list_.push_back(std::make_pair(key, value));
      return true;
    };

   private:
    // TODO(student): You may add additional private members and helper functions
    size_t size_;
    int depth_;
    int common_bits_ = -1;  // -1 indicates no value
    std::list<std::pair<K, V>> list_;
    std::shared_ptr<Bucket> next_bucket_;  // next bucket pointer
    std::shared_mutex busket_mutex_;       // The mutex of the bucket for read and write
  };

 private:
  // TODO(student): You may add additional private members and helper functions and remove the ones
  // you don't need.

  int global_depth_;    // The global depth of the directory
  size_t bucket_size_;  // The size of a bucket
  int num_buckets_;     // The number of buckets in the hash table
  mutable std::mutex latch_;
  std::shared_mutex dir_mutex_;               // The mutex of the directory
  std::vector<std::shared_ptr<Bucket>> dir_;  // The directory of the hash table

  // The following functions are completely optional, you can delete them if you have your own ideas.

  /**
   * @brief Redistribute the kv pairs in a full bucket.
   * @param bucket The bucket to be redistributed.
   */
  auto RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void;

  /*****************************************************************
   * Must acquire latch_ first before calling the below functions. *
   *****************************************************************/

  /**
   * @brief For the given key, return the entry index in the directory where the key hashes to.
   * @param key The key to be hashed.
   * @return The entry index in the directory.
   */
  auto IndexOf(const K &key) -> size_t;

  auto GetGlobalDepthInternal() const -> int;
  auto GetLocalDepthInternal(int dir_index) const -> int;
  auto GetNumBucketsInternal() const -> int;
};

}  // namespace bustub
