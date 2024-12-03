//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void DeleteExecutor::Init() {
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
    exec_ctx_->GetTransaction()->SetState(TransactionState::ABORTED);
    throw ExecutionException("InsertExecutor::Init() abort");
  }
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  is_end_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // Get the next tuple
  Tuple to_delete_tuple{};
  RID to_delete_rid;
  int32_t delete_count = 0;
  if (is_end_) {
    return false;
  }
  while (child_executor_->Next(&to_delete_tuple, &to_delete_rid)) {
    try {
      // 获取行锁 排它锁X 为什么不用解锁呢？在哪儿解锁的 事务提交的时候解锁
      bool is_locked = exec_ctx_->GetLockManager()->LockRow(
          exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE, table_info_->oid_, to_delete_rid);
      if (!is_locked) {
        exec_ctx_->GetTransaction()->SetState(TransactionState::ABORTED);
        throw ExecutionException("Delete Executor Get Row Lock Failed");
      }
    } catch (TransactionAbortException const &e) {
      exec_ctx_->GetTransaction()->SetState(TransactionState::ABORTED);
      throw ExecutionException("Delete Executor Get Row Lock Failed");
    }
    bool deleted = table_info_->table_->MarkDelete(to_delete_rid, exec_ctx_->GetTransaction());
    if (deleted) {  // Delete the tuple from the indexes
      for (auto index_info_itr : table_indexes_) {
        auto tuple_deleted = to_delete_tuple.KeyFromTuple(table_info_->schema_, index_info_itr->key_schema_,
                                                          index_info_itr->index_->GetKeyAttrs());
        index_info_itr->index_->DeleteEntry(tuple_deleted, to_delete_rid, exec_ctx_->GetTransaction());
      }
      delete_count++;
    }
  }
  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, delete_count);
  *tuple = Tuple{values, &GetOutputSchema()};
  is_end_ = true;
  return true;
}

}  // namespace bustub
