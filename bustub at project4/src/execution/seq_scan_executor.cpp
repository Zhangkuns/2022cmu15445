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
  LockTable();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // Get the next tuple from the table
  if (table_iterator_ == table_info_->table_->End()) {
    UnLockTable();
    return false;
  }
  *rid = table_iterator_->GetRid();
  LockRow(*rid);
  *tuple = *table_iterator_;
  UnLockRow(*rid);
  ++table_iterator_;
  return true;
}

void SeqScanExecutor::LockTable() {
  const auto &txn = exec_ctx_->GetTransaction();
  const auto &lock_mgr = exec_ctx_->GetLockManager();
  const auto &oid = plan_->table_oid_;
  bool res = true;
  try {
    if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      if (!txn->IsTableExclusiveLocked(oid) || !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
        res = lock_mgr->LockTable(txn, LockManager::LockMode::INTENTION_SHARED, oid);
      }
      if (!res) {
        txn->SetState(TransactionState::ABORTED);
        throw ExecutionException("SeqScanExecutor::Init() lock fail");
      }
    }
  } catch (TransactionAbortException &e) {
    txn->SetState(TransactionState::ABORTED);
    throw ExecutionException("SeqScanExecutor::Init() lock fail");
  }
}

void SeqScanExecutor::UnLockTable() {
  const auto &txn = exec_ctx_->GetTransaction();
  const auto &lock_mgr = exec_ctx_->GetLockManager();
  const auto &oid = plan_->table_oid_;
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->IsTableIntentionSharedLocked(oid)) {
      lock_mgr->UnlockTable(txn, oid);
    }
  }
}

void SeqScanExecutor::LockRow(const RID &rid) {
  const auto &txn = exec_ctx_->GetTransaction();
  const auto &lock_mgr = exec_ctx_->GetLockManager();
  const auto &oid = plan_->GetTableOid();
  bool res = true;
  try {
    if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      if (!txn->IsRowExclusiveLocked(oid, rid)) {
        res = lock_mgr->LockRow(txn, LockManager::LockMode::SHARED, oid, rid);
      }
      if (!res) {
        txn->SetState(TransactionState::ABORTED);
        throw ExecutionException("SeqScanExecutor::Init() lock fail");
      }
    }
  } catch (TransactionAbortException &e) {
    txn->SetState(TransactionState::ABORTED);
    throw ExecutionException("SeqScanExecutor::Init() lock fail");
  }
}

void SeqScanExecutor::UnLockRow(const RID &rid) {
  const auto &txn = exec_ctx_->GetTransaction();
  const auto &lock_mgr = exec_ctx_->GetLockManager();
  const auto &oid = plan_->table_oid_;
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->IsRowSharedLocked(oid, rid)) {
      lock_mgr->UnlockRow(txn, oid, rid);
    }
  }
}

}  // namespace bustub
