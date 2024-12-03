//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 24
#define INTERNAL_PAGE_SIZE ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType)))
/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  // must call initialize method after "create" a new node
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = INTERNAL_PAGE_SIZE);

  auto KeyAt(int index) const -> KeyType;
  void SetKeyAt(int index, const KeyType &key);
  auto ValueAt(int index) const -> ValueType;
  auto SetValueAt(int index, const ValueType &value) -> void;
  auto ElemntAt(int index) const -> MappingType;
  auto ValueIndex(const ValueType &value) const -> int;
  auto FindIndexAboveKey(const KeyType &key, const KeyComparator &comparator) const -> int;
  auto FindKeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int;
  auto SetNewRoot(ValueType &old_value, const KeyType &new_key, const ValueType &new_value) -> void;
  auto InsertNodeAfter(ValueType &old_value, const KeyType &new_key, const ValueType &new_value) -> void;
  auto DeleteNode(const KeyType &key, const KeyComparator &comparator) -> bool;
  void MoveAllTo(B_PLUS_TREE_INTERNAL_PAGE_TYPE *recipient, const KeyType &middle_key,
                 BufferPoolManager *buffer_pool_manager);
  void CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager);
  void MoveLastHalfTo(B_PLUS_TREE_INTERNAL_PAGE_TYPE *recipient, int spiltindex,
                      BufferPoolManager *buffer_pool_manager);
  void CopyFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager);
  void MoveLastToFrontOf(B_PLUS_TREE_INTERNAL_PAGE_TYPE *recipient, KeyType k, BufferPoolManager *buffer_pool_manager);
  void MoveFirstToEndOf(B_PLUS_TREE_INTERNAL_PAGE_TYPE *recipient, KeyType middlekey,
                        BufferPoolManager *buffer_pool_manager);
  void CopyFirstFrom(const MappingType &item, BufferPoolManager *buffer_pool_manager);
  void CopyLastFrom(const MappingType &item, BufferPoolManager *buffer_pool_manager);

 private:
  // Flexible array member for page data.
  /**
   *The size of 1 is a placeholder,
   * and the actual size of the array will be determined when an instance of the `BPlusTreeInternalPage` class is
   *created.
   */
  MappingType array_[10];
};
}  // namespace bustub