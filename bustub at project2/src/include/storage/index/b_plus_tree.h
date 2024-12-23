//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>
#include <utility>
#include <vector>
#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  enum class Operation { SEARCH, INSERT, DELETE };

 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  [[nodiscard]] auto IsEmpty() const -> bool;

  // return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr) -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;

  // Insert into parent node after split
  auto InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                        Transaction *transaction = nullptr) -> void;

  // Insert a key-value pair into empty B+ tree.
  auto StartNewTree(const KeyType &key, const ValueType &value, Transaction *transaction) -> void;

  // Spilt a node into two and return the new node's page_id
  template <typename N>
  auto Split(N *node) -> N *;

  // coalesce or redistribute pages if their size is not enough
  template <typename N>
  auto CoalesceOrRedistribute(N *node, Transaction *transaction) -> bool;

  // Coalesce a node into its sibling
  template <typename N>
  void Coalesce(N *node, N *sibling, InternalPage *parent, int index, KeyType k, Transaction *transaction);

  // Redistribute records between two nodes
  template <typename N>
  void Redistribute(N *node, N *sibling, InternalPage *parent, int index, Transaction *transaction);

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key range
  auto GetValueRange(const KeyType &left_key, const KeyType &right_key, std::vector<ValueType> *result,
                     Transaction *transaction = nullptr) -> bool;

  // return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // delete entry from the B+ tree, return false if key is not in the tree
  template <typename N>
  auto DeleteEntry(N *node, const KeyType &key, const KeyComparator &comparator, Transaction *transaction) -> bool;

  // index iterator
  auto Begin() -> INDEXITERATOR_TYPE;
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;
  auto End() -> INDEXITERATOR_TYPE;

  // release the latch of ancestor pages
  auto ReleaseLatchFromQueue(Transaction *transaction) -> void;

  // Get the number of whole pairs in the tree
  auto GetSize() -> int;

  // print the B+ tree
  void Print(BufferPoolManager *bpm);

  // draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // helper function to find the leaf page
  auto FindLeafPage(const KeyType &key) -> Page *;

  // helper function to find the leaf page concurrency
  auto FindLeafPageCon(const KeyType &key, Operation op, Transaction *transaction) -> Page *;

  auto IsBalanced(page_id_t pid) -> int;
  auto IsPageCorr(page_id_t pid, std::pair<KeyType, KeyType> &out) -> bool;
  auto IsUnlocked(page_id_t pid, std::pair<KeyType, KeyType> &out) -> bool;
  auto Check(bool forcecheck) -> bool;
  bool open_check_ = true;

 private:
  void UpdateRootPageId(int insert_record = 0);

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  auto FetchPage(page_id_t page_id) -> BPlusTreePage *;

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
  ReaderWriterLatch root_page_id_latch_;
};

}  // namespace bustub
