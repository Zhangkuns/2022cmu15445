//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void InsertExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  child_executor_->Init();
  try {
    bool is_locked = exec_ctx_->GetLockManager()->LockTable(
        exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_);
    if (!is_locked) {
      exec_ctx_->GetTransaction()->SetState(TransactionState::ABORTED);
      throw ExecutionException("Insert Executor Get Table Lock Failed");
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("InsertExecutor::Init() abort");
  }
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  is_end_ = false;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // Get the next tuple
  Tuple to_insert_tuple{};
  RID to_insert_rid;
  int32_t insert_count = 0;
  if (is_end_) {
    return false;
  }
  while (child_executor_->Next(&to_insert_tuple, &to_insert_rid)) {
    // Insert the tuple into the table
    bool inserted = table_info_->table_->InsertTuple(to_insert_tuple, &to_insert_rid, exec_ctx_->GetTransaction());
    if (inserted) {
      try {
        bool is_locked = exec_ctx_->GetLockManager()->LockRow(
            exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE, table_info_->oid_, to_insert_rid);
        if (!is_locked) {
          exec_ctx_->GetTransaction()->SetState(TransactionState::ABORTED);
          throw ExecutionException("Insert Executor Get Row Lock Failed");
        }
      } catch (TransactionAbortException &e) {
        exec_ctx_->GetTransaction()->SetState(TransactionState::ABORTED);
        throw ExecutionException("Insert Executor Get Row Lock Failed");
      }
      // Insert the tuple into the indexes
      for (auto index_info_itr : table_indexes_) {
        auto tuple_inserted = to_insert_tuple.KeyFromTuple(table_info_->schema_, index_info_itr->key_schema_,
                                                           index_info_itr->index_->GetKeyAttrs());
        index_info_itr->index_->InsertEntry(tuple_inserted, to_insert_rid, exec_ctx_->GetTransaction());
      }
      insert_count++;
    }
  }
  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, insert_count);
  *tuple = Tuple{values, &GetOutputSchema()};
  is_end_ = true;
  return true;
}

}  // namespace bustub
