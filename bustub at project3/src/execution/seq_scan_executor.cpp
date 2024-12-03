//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  table_iterator_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}

void SeqScanExecutor::Init() {
  // Initialize the table iterator
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  table_iterator_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // Get the next tuple from the table
  if (table_iterator_ == table_info_->table_->End()) {
    return false;
  }
  *rid = table_iterator_->GetRid();
  *tuple = *table_iterator_;
  ++table_iterator_;
  return true;
}

}  // namespace bustub
