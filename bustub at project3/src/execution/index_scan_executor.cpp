//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      index_info_{this->exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)},
      table_info_{this->exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_)},
      tree_{dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get())},
      index_iterator_{tree_->GetBeginIterator()} {}

void IndexScanExecutor::Init() {
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
  tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());
  index_iterator_ = tree_->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_iterator_ == tree_->GetEndIterator()) {
    return false;
  }
  *rid = (*index_iterator_).second;
  auto result = table_info_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
  ++index_iterator_;
  return result;
}

}  // namespace bustub
