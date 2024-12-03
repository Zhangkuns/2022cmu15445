//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), left_executor_(std::move(child_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  /*索引信息和表信息，child是左表，右表是当前，并且有索引*/
  index_info_ = exec_ctx->GetCatalog()->GetIndex(plan_->GetIndexOid());
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan_->GetInnerTableOid());
}

void NestIndexJoinExecutor::Init() {
  // Initialize the child executors
  Tuple tuple;
  RID rid;
  left_executor_->Init();
  while (left_executor_->Next(&tuple, &rid)) {
    left_tuples_.push_back(tuple);
  }
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple *left_tuple;
  if ((plan_->GetJoinType() == JoinType::INNER)) {
    while (true) {
      if (left_index_ == left_tuples_.size()) {
        return false;
      }
      left_tuple = &left_tuples_[left_index_];
      auto key_schema = index_info_->index_->GetKeySchema();  // key_schema是索引的schema
      auto value = plan_->KeyPredicate()->Evaluate(left_tuple, left_executor_->GetOutputSchema());
      std::vector<Value> values;
      values.push_back(value);
      Tuple key(values, key_schema);
      std::vector<RID> results;
      index_info_->index_->ScanKey(key, &results, exec_ctx_->GetTransaction());
      if (!results.empty()) {
        for (const auto &rid_b : results) {
          Tuple right_tuple;
          if (table_info_->table_->GetTuple(rid_b, &right_tuple, exec_ctx_->GetTransaction())) {
            std::vector<Value> tuple_values;
            for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
              tuple_values.push_back(left_tuple->GetValue(&left_executor_->GetOutputSchema(), i));
            }
            for (uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
              tuple_values.push_back(right_tuple.GetValue(&table_info_->schema_, i));
            }
            *tuple = {tuple_values, &plan_->OutputSchema()};
            left_index_++;
            return true;
          }
        }
      }
      left_index_++;
    }
  }

  if ((plan_->GetJoinType() == JoinType::LEFT)) {
    while (true) {
      if (left_index_ == left_tuples_.size()) {
        return false;
      }
      left_tuple = &left_tuples_[left_index_];
      auto key_schema = index_info_->index_->GetKeySchema();
      /*这个tuple对应的schema*/
      auto value = plan_->KeyPredicate()->Evaluate(left_tuple, left_executor_->GetOutputSchema());
      std::vector<Value> values;
      values.push_back(value);
      Tuple key(values, key_schema);
      std::vector<RID> results;
      index_info_->index_->ScanKey(key, &results, exec_ctx_->GetTransaction());
      if (!results.empty()) {
        for (const auto &rid_b : results) {
          Tuple right_tuple;
          if (table_info_->table_->GetTuple(rid_b, &right_tuple, exec_ctx_->GetTransaction())) {
            std::vector<Value> tuple_values;
            for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
              tuple_values.push_back(left_tuple->GetValue(&left_executor_->GetOutputSchema(), i));
            }
            for (uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
              tuple_values.push_back(right_tuple.GetValue(&table_info_->schema_, i));
            }
            *tuple = {tuple_values, &plan_->OutputSchema()};
            left_index_++;
            return true;
          }
        }
      } else {
        std::vector<Value> tuple_values;
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
          tuple_values.push_back(left_tuple->GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
          tuple_values.push_back(ValueFactory::GetNullValueByType(table_info_->schema_.GetColumn(i).GetType()));
        }
        *tuple = {tuple_values, &plan_->OutputSchema()};
        left_index_++;
        return true;
      }
    }
  }

  if (plan_->GetJoinType() != JoinType::LEFT && plan_->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan_->GetJoinType()));
  }
  return false;
}

}  // namespace bustub
