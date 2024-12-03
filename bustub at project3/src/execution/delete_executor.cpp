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
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
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
