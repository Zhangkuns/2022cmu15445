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
#include <cassert>
#include "common/config.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"

// #define SEQNLOCK

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())),
      cur_(nullptr, {}, nullptr) {}

void SeqScanExecutor::Init() {
  cur_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
  LockTable();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  const auto &end = table_info_->table_->End();

  while (cur_ != end) {
    *rid = cur_->GetRid();

    LockRow(*rid);
    *tuple = *cur_;
    UnLockRow(*rid);

    cur_++;
    if (plan_->filter_predicate_ != nullptr) {
      const auto value = plan_->filter_predicate_->Evaluate(tuple, plan_->OutputSchema());
      if (value.IsNull() || !value.GetAs<bool>()) {
        continue;
      }
    }
    // fmt::print("SeqScanExecutor::Next {}\n", tuple->ToString(&plan_->OutputSchema()));

    return true;
  }

  UnLockTable();
  return false;
}

void SeqScanExecutor::LockTable() {
#ifndef SEQNLOCK
  const auto &txn = exec_ctx_->GetTransaction();
  const auto &lock_mgr = exec_ctx_->GetLockManager();
  const auto &oid = plan_->table_oid_;
  bool res = true;

  try {
    if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      if (!txn->IsTableIntentionExclusiveLocked(oid) && !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
        res = lock_mgr->LockTable(txn, LockManager::LockMode::INTENTION_SHARED, oid);
      }
    }
    if (!res) {
      assert(txn->GetState() == TransactionState::ABORTED);
      throw ExecutionException("SeqScanExecutor::Init() lock fail");
    }
  } catch (TransactionAbortException &e) {
    assert(txn->GetState() == TransactionState::ABORTED);
    throw ExecutionException("SeqScanExecutor::Init() lock fail");
  }
#endif
}

void SeqScanExecutor::UnLockTable() {
#ifndef SEQNLOCK
  const auto &txn = exec_ctx_->GetTransaction();
  const auto &lock_mgr = exec_ctx_->GetLockManager();
  const auto &oid = plan_->GetTableOid();
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->IsTableIntentionSharedLocked(oid)) {
      lock_mgr->UnlockTable(txn, oid);
    }
  }
#endif
}

void SeqScanExecutor::LockRow(const RID &rid) {
#ifndef SEQNLOCK
  const auto &txn = exec_ctx_->GetTransaction();
  const auto &lock_mgr = exec_ctx_->GetLockManager();
  const auto &oid = plan_->GetTableOid();
  bool res = true;
  if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    if (!txn->IsRowExclusiveLocked(oid, rid)) {
      res = lock_mgr->LockRow(txn, LockManager::LockMode::SHARED, oid, rid);
    }
  }
  if (!res) {
    txn->SetState(TransactionState::ABORTED);
    throw ExecutionException("SeqScanExecutor::Next() lock fail");
  }
#endif
}

void SeqScanExecutor::UnLockRow(const RID &rid) {
#ifndef SEQNLOCK
  const auto &txn = exec_ctx_->GetTransaction();
  const auto &lock_mgr = exec_ctx_->GetLockManager();
  const auto &oid = plan_->GetTableOid();
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->IsRowSharedLocked(oid, rid)) {
      lock_mgr->UnlockRow(txn, oid, rid);
    }
  }
#endif
}

}  // namespace bustub
